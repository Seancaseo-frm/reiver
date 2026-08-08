//! Slack App integration via OAuth 2.0.
//!
//! Provides:
//! - OAuth install flow (Add to Slack)
//! - OAuth callback (exchange code for bot token)
//! - Events API endpoint (uninstall, token revocation, @mentions)
//! - Account linking for permission-scoped agent access
//! - Slack-to-agent bridge for interactive Moodeng conversations

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Redirect, Response},
    routing::{get, post},
    Router,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use tracing::{error, info, info_span, warn, Instrument};
use uuid::Uuid;

use bb8_redis::redis::AsyncCommands;

use reiver_core::audit::{AuditCaller, AuditEventBuilder, AuditEventType, AuditOrigin};

use crate::app_state::WatchState;
use crate::error::{AppError, Result};

const CHANNEL_TYPE: &str = "slack";
const CSRF_STATE_TTL_SECONDS: u64 = 600;
const REDIS_KEY_SLACK_CSRF: &str = "slack:csrf";
const LINK_TOKEN_TTL_SECONDS: u64 = 600;

type HmacSha256 = Hmac<Sha256>;

// ─── Router ──────────────────────────────────────────────────────────────────

pub fn create_slack_router() -> Router<Arc<WatchState>> {
    Router::new()
        .route("/oauth/install", get(oauth_install))
        .route("/oauth/callback", get(oauth_callback))
        .route("/oauth/finalize", post(finalize_install))
        .route("/integrations", get(list_integrations))
        .route(
            "/integrations/{id}",
            get(get_integration).delete(delete_integration),
        )
        .route("/events", post(handle_events))
        .route("/interactivity", post(handle_interactivity))
        .route("/link", get(link_account))
}

// ─── CSRF helpers (mirrors github.rs pattern) ────────────────────────────────

fn generate_csrf_state() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

async fn store_csrf_state(
    redis: &Arc<bb8::Pool<bb8_redis::RedisConnectionManager>>,
    token: &str,
    user_id: Option<Uuid>,
    project_id: Option<Uuid>,
) -> Result<()> {
    let mut conn = redis
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis: {}", e)))?;
    let key = format!("{}:{}", REDIS_KEY_SLACK_CSRF, token);
    let value = match (user_id, project_id) {
        (Some(u), Some(p)) => format!("{}:{}", u, p),
        _ => "marketplace".to_string(),
    };
    let _: () = conn
        .set_ex(&key, value, CSRF_STATE_TTL_SECONDS)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis set: {}", e)))?;
    Ok(())
}

/// Returns (user_id, project_id) if both were stored, or (None, None) for marketplace installs.
async fn validate_csrf_state(
    redis: &Arc<bb8::Pool<bb8_redis::RedisConnectionManager>>,
    token: &str,
) -> Result<(Option<Uuid>, Option<Uuid>)> {
    let mut conn = redis
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis: {}", e)))?;
    let key = format!("{}:{}", REDIS_KEY_SLACK_CSRF, token);
    let stored: Option<String> = conn
        .get_del(&key)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis get: {}", e)))?;
    let stored =
        stored.ok_or_else(|| AppError::BadRequest("Invalid or expired state token".into()))?;
    if stored == "marketplace" {
        return Ok((None, None));
    }
    let parts: Vec<&str> = stored.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(AppError::Internal(anyhow::anyhow!("Corrupt CSRF state")));
    }
    let user_id = parts[0]
        .parse::<Uuid>()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Bad user_id in CSRF state")))?;
    let project_id = parts[1]
        .parse::<Uuid>()
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Bad project_id in CSRF state")))?;
    Ok((Some(user_id), Some(project_id)))
}

// ─── OAuth Install ───────────────────────────────────────────────────────────

/// GET /slack/oauth/install — 302 redirect to Slack authorize page.
///
/// Works in two modes:
/// - **In-app** (authenticated, `project_id` present): stores user+project in
///   CSRF state so the callback can persist the integration immediately.
/// - **Marketplace / Direct Install** (unauthenticated, no `project_id`): stores
///   a "marketplace" CSRF state. After the callback the user is sent to
///   `/slack/install?pending=<key>` to log in, pick a project, and finalize.
///
/// The Direct Install URL *must* return a 302 to `slack.com` (Slack requirement).
#[tracing::instrument(name = "slack.oauth.install", skip_all, fields(project_id))]
async fn oauth_install(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(query): Query<OAuthInstallQuery>,
) -> Result<Response> {
    let user_id = crate::api::extract_user_id(&headers).ok();
    let project_id = crate::api::extract_project_id(&headers).ok().or_else(|| {
        query
            .project_id
            .as_deref()
            .and_then(|pid| pid.parse::<Uuid>().ok())
    });
    if let Some(pid) = project_id {
        tracing::Span::current().record("project_id", tracing::field::display(pid));
    }

    let client_id = state
        .config
        .slack_client_id
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("SLACK_CLIENT_ID not configured")))?;
    let base_url = state
        .config
        .api_base_url
        .as_deref()
        .unwrap_or("http://localhost:3000");

    let csrf = generate_csrf_state();
    store_csrf_state(&state.redis, &csrf, user_id, project_id).await?;

    let redirect_uri = format!("{}/api/slack/oauth/callback", base_url);
    let url = format!(
        "https://slack.com/oauth/v2/authorize?client_id={}&scope=chat:write,app_mentions:read,reactions:write,reactions:read,im:history,im:write,assistant:write,incoming-webhook&redirect_uri={}&state={}",
        client_id,
        urlencoding::encode(&redirect_uri),
        csrf
    );

    info!(user_id = ?user_id, project_id = ?project_id, "Redirecting to Slack OAuth");
    Ok((StatusCode::FOUND, [(axum::http::header::LOCATION, url)]).into_response())
}

// ─── OAuth Install Query ──────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct OAuthInstallQuery {
    project_id: Option<String>,
}

// ─── OAuth Callback ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct SlackOAuthResponse {
    ok: bool,
    access_token: Option<String>,
    team: Option<SlackTeam>,
    bot_user_id: Option<String>,
    incoming_webhook: Option<SlackIncomingWebhook>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct SlackTeam {
    id: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize)]
struct SlackIncomingWebhook {
    channel: Option<String>,
    channel_id: Option<String>,
}

const PENDING_INSTALL_TTL_SECONDS: u64 = 600;
const REDIS_KEY_SLACK_PENDING: &str = "slack:pending";

/// GET /slack/oauth/callback — exchange code for token, store integration.
///
/// Two modes:
/// - **In-app** (project_id in state): persists integration to DB immediately.
/// - **Marketplace** (no project_id): stores the encrypted token in Redis as a
///   "pending install" and redirects to `/slack/install?pending=<key>` where
///   the user logs in, picks a project, and finalizes via POST /slack/oauth/finalize.
#[tracing::instrument(
    name = "slack.oauth.callback",
    skip_all,
    fields(team_id, is_marketplace)
)]
async fn oauth_callback(
    State(state): State<Arc<WatchState>>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Result<Response> {
    if let Some(ref err) = query.error {
        warn!(error = %err, "Slack OAuth denied by user");
        return Ok(Redirect::temporary("/slack/install?slack=denied").into_response());
    }

    let code = query
        .code
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("Missing code parameter".into()))?;
    let csrf_token = query
        .state
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("Missing state parameter".into()))?;

    let (opt_user_id, opt_project_id) = validate_csrf_state(&state.redis, csrf_token).await?;

    let client_id = state
        .config
        .slack_client_id
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("SLACK_CLIENT_ID not configured")))?;
    let client_secret =
        state.config.slack_client_secret.as_ref().ok_or_else(|| {
            AppError::Internal(anyhow::anyhow!("SLACK_CLIENT_SECRET not configured"))
        })?;
    let base_url = state
        .config
        .api_base_url
        .as_deref()
        .unwrap_or("http://localhost:3000");
    let redirect_uri = format!("{}/api/slack/oauth/callback", base_url);

    let http = reqwest::Client::new();
    let resp = http
        .post("https://slack.com/api/oauth.v2.access")
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
        ])
        .send()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Slack API error: {}", e)))?;

    let oauth: SlackOAuthResponse = resp.json().await.map_err(|e| {
        AppError::Internal(anyhow::anyhow!("Failed to parse Slack response: {}", e))
    })?;

    if !oauth.ok {
        let err = oauth.error.unwrap_or_else(|| "unknown".into());
        error!(error = %err, "Slack OAuth token exchange failed");
        return Err(AppError::Internal(anyhow::anyhow!(
            "Slack OAuth failed: {}",
            err
        )));
    }

    let access_token = oauth
        .access_token
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("No access_token in Slack response")))?;
    let team = oauth.team.unwrap_or(SlackTeam {
        id: None,
        name: None,
    });
    let team_id = team.id.unwrap_or_default();
    let team_name = team.name.unwrap_or_default();
    let bot_user_id = oauth.bot_user_id.unwrap_or_default();
    let span = tracing::Span::current();
    span.record("team_id", &team_id.as_str());
    span.record("is_marketplace", opt_project_id.is_none());
    let webhook = oauth.incoming_webhook.unwrap_or(SlackIncomingWebhook {
        channel: None,
        channel_id: None,
    });
    let channel = webhook.channel.unwrap_or_default();
    let channel_id = webhook.channel_id.unwrap_or_default();

    let encrypted_token = state
        .encryptor
        .encrypt(&access_token)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Encryption failed: {}", e)))?;

    let config = serde_json::json!({
        "bot_token": encrypted_token,
        "team_id": team_id,
        "team_name": team_name,
        "channel": channel,
        "channel_id": channel_id,
        "bot_user_id": bot_user_id,
    });

    // In-app flow: project_id known, persist immediately
    if let Some(project_id) = opt_project_id {
        let name = format!("Slack — {}", team_name);

        sqlx::query(
            r#"INSERT INTO notification_channels (project_id, name, channel_type, config, enabled)
               VALUES ($1, $2, $3, $4, true)
               ON CONFLICT (project_id, name)
               DO UPDATE SET config = $4, enabled = true, updated_at = NOW()"#,
        )
        .bind(project_id)
        .bind(&name)
        .bind(CHANNEL_TYPE)
        .bind(&config)
        .execute(state.db.as_ref())
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("DB insert failed: {}", e)))?;

        info!(%project_id, %team_id, "Slack OAuth integration installed");
        let redirect = format!("/projects/{}/integrations?slack=installed", project_id);
        return Ok(Redirect::temporary(&redirect).into_response());
    }

    // Marketplace flow: no project_id yet — store as pending install in Redis
    let pending_key = generate_csrf_state();
    let mut conn = state
        .redis
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis: {}", e)))?;
    let redis_key = format!("{}:{}", REDIS_KEY_SLACK_PENDING, pending_key);
    let _: () = conn
        .set_ex(&redis_key, config.to_string(), PENDING_INSTALL_TTL_SECONDS)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis set: {}", e)))?;

    info!(%team_id, "Slack OAuth token stored as pending install");
    let redirect = format!("/slack/install?pending={}", pending_key);
    Ok(Redirect::temporary(&redirect).into_response())
}

// ─── Finalize pending install ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct FinalizeRequest {
    pending_key: String,
    project_id: Uuid,
}

/// POST /slack/oauth/finalize — associate a pending marketplace install with a project.
///
/// Called by the frontend after the user logs in and picks a project.
/// Reads the pending install data from Redis and persists it to the DB.
#[tracing::instrument(name = "slack.oauth.finalize", skip_all, fields(project_id = %body.project_id))]
async fn finalize_install(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Json(body): Json<FinalizeRequest>,
) -> Result<Json<serde_json::Value>> {
    let _user_id = crate::api::extract_user_id(&headers)?;

    let mut conn = state
        .redis
        .get()
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis: {}", e)))?;
    let redis_key = format!("{}:{}", REDIS_KEY_SLACK_PENDING, body.pending_key);
    let stored: Option<String> = conn
        .get_del(&redis_key)
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis get: {}", e)))?;

    let config_str = stored.ok_or_else(|| {
        AppError::BadRequest("Pending install expired or already finalized".into())
    })?;

    let config: serde_json::Value = serde_json::from_str(&config_str)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Corrupt pending data: {}", e)))?;

    let team_name = config["team_name"].as_str().unwrap_or("Unknown");
    let team_id = config["team_id"].as_str().unwrap_or("");
    let name = format!("Slack — {}", team_name);

    sqlx::query(
        r#"INSERT INTO notification_channels (project_id, name, channel_type, config, enabled)
           VALUES ($1, $2, $3, $4, true)
           ON CONFLICT (project_id, name)
           DO UPDATE SET config = $4, enabled = true, updated_at = NOW()"#,
    )
    .bind(body.project_id)
    .bind(&name)
    .bind(CHANNEL_TYPE)
    .bind(&config)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB insert failed: {}", e)))?;

    info!(project_id = %body.project_id, %team_id, "Slack pending install finalized");

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationCreated)
        .actor(_user_id)
        .resource("slack", body.project_id)
        .details(serde_json::json!({ "created": { "team_id": team_id, "team_name": team_name } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(Json(serde_json::json!({
        "ok": true,
        "project_id": body.project_id,
        "team_name": team_name,
    })))
}

// ─── List / Get / Delete integrations ────────────────────────────────────────

#[derive(Serialize)]
struct SlackIntegrationResponse {
    id: Uuid,
    project_id: Uuid,
    name: String,
    team_id: String,
    team_name: String,
    channel: String,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn config_to_response(
    id: Uuid,
    project_id: Uuid,
    name: String,
    config: &serde_json::Value,
    enabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
) -> SlackIntegrationResponse {
    SlackIntegrationResponse {
        id,
        project_id,
        name,
        team_id: config["team_id"].as_str().unwrap_or("").into(),
        team_name: config["team_name"].as_str().unwrap_or("").into(),
        channel: config["channel"].as_str().unwrap_or("").into(),
        enabled,
        created_at,
    }
}

#[tracing::instrument(name = "slack.integration.list", skip_all)]
async fn list_integrations(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<SlackIntegrationResponse>>> {
    let project_id = crate::api::extract_project_id(&headers)?;

    let rows = sqlx::query_as::<_, (Uuid, Uuid, String, serde_json::Value, bool, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, project_id, name, config, enabled, created_at FROM notification_channels WHERE project_id = $1 AND channel_type = $2 ORDER BY created_at DESC",
    )
    .bind(project_id)
    .bind(CHANNEL_TYPE)
    .fetch_all(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB: {}", e)))?;

    let items: Vec<SlackIntegrationResponse> = rows
        .into_iter()
        .map(|(id, pid, name, config, enabled, created)| {
            config_to_response(id, pid, name, &config, enabled, created)
        })
        .collect();
    Ok(Json(items))
}

#[tracing::instrument(name = "slack.integration.get", skip_all, fields(id = %id))]
async fn get_integration(
    State(state): State<Arc<WatchState>>,
    Path(id): Path<Uuid>,
) -> Result<Json<SlackIntegrationResponse>> {
    let row = sqlx::query_as::<_, (Uuid, Uuid, String, serde_json::Value, bool, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, project_id, name, config, enabled, created_at FROM notification_channels WHERE id = $1 AND channel_type = $2",
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB: {}", e)))?
    .ok_or_else(|| AppError::NotFound("Slack integration not found".into()))?;

    Ok(Json(config_to_response(
        row.0, row.1, row.2, &row.3, row.4, row.5,
    )))
}

#[tracing::instrument(name = "slack.integration.delete", skip_all, fields(id = %id))]
async fn delete_integration(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let deleted_row = sqlx::query_as::<_, (Uuid, Uuid, String, serde_json::Value, bool, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, project_id, name, config, enabled, created_at FROM notification_channels WHERE id = $1 AND channel_type = $2"
    )
    .bind(id)
    .bind(CHANNEL_TYPE)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten();

    let result =
        sqlx::query("DELETE FROM notification_channels WHERE id = $1 AND channel_type = $2")
            .bind(id)
            .bind(CHANNEL_TYPE)
            .execute(state.db.as_ref())
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("DB: {}", e)))?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Slack integration not found".into()));
    }
    info!(%id, "Slack integration deleted");

    let audit_origin = AuditOrigin::from_headers(&headers);
    let audit_caller = AuditCaller::from_headers(&headers);
    AuditEventBuilder::new(AuditEventType::IntegrationDeleted)
        .resource("slack", id)
        .details(serde_json::json!({ "deleted": { "name": deleted_row.as_ref().map(|r| &r.2) } }))
        .origin(
            &audit_origin.origin_type,
            &audit_origin.origin_ref,
            &audit_origin.origin_reason,
        )
        .caller(
            &audit_caller.caller_type,
            &audit_caller.key_label,
            &audit_caller.key_prefix,
        )
        .success()
        .log(&state.clickhouse)
        .await;

    Ok(StatusCode::NO_CONTENT)
}

// ─── Events API ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SlackEventPayload {
    #[serde(rename = "type")]
    event_type: String,
    challenge: Option<String>,
    team_id: Option<String>,
    event_id: Option<String>,
    event: Option<SlackEvent>,
}

#[derive(Deserialize)]
struct SlackEvent {
    #[serde(rename = "type")]
    event_type: String,
    subtype: Option<String>,
    user: Option<String>,
    text: Option<String>,
    channel: Option<String>,
    channel_type: Option<String>,
    ts: Option<String>,
    thread_ts: Option<String>,
    /// Only present on bot's own messages — skip them to avoid loops
    bot_id: Option<String>,
    /// Present on `assistant_thread_started` events
    assistant_thread: Option<AssistantThread>,
    /// Nested message object — present on `message_changed` / `message_deleted` subtypes.
    /// Slack's Assistant API intermittently delivers user messages as `message_changed`
    /// events where the real user + text live inside this nested object.
    message: Option<Box<SlackNestedMessage>>,
}

#[derive(Deserialize)]
struct SlackNestedMessage {
    user: Option<String>,
    text: Option<String>,
    bot_id: Option<String>,
    thread_ts: Option<String>,
}

#[derive(Deserialize)]
struct AssistantThread {
    channel_id: Option<String>,
    thread_ts: Option<String>,
}

fn verify_slack_signature(
    signing_secret: &str,
    timestamp: &str,
    body: &[u8],
    expected_sig: &str,
) -> bool {
    let base = format!("v0:{}:{}", timestamp, String::from_utf8_lossy(body));
    let mut mac = match HmacSha256::new_from_slice(signing_secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(base.as_bytes());
    let computed = format!("v0={}", hex::encode(mac.finalize().into_bytes()));
    computed == expected_sig
}

/// POST /slack/events — Slack Events API receiver.
#[tracing::instrument(
    name = "slack.event",
    skip_all,
    fields(event_type, team_id, event_id, inner_event_type, inner_subtype)
)]
async fn handle_events(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    info!(
        body_len = body.len(),
        has_signature = headers.get("X-Slack-Signature").is_some(),
        has_timestamp = headers.get("X-Slack-Request-Timestamp").is_some(),
        retry = ?headers.get("X-Slack-Retry-Num"),
        "Slack events endpoint hit"
    );

    // Verify signature
    if let Some(ref secret) = state.config.slack_signing_secret {
        let timestamp = headers
            .get("X-Slack-Request-Timestamp")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let signature = headers
            .get("X-Slack-Signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !verify_slack_signature(secret, timestamp, &body, signature) {
            warn!("Slack signature verification failed");
            return Err(AppError::Forbidden("Invalid Slack signature".into()));
        }
    }

    let payload: SlackEventPayload = serde_json::from_slice(&body)
        .map_err(|e| {
            error!(error = %e, body = %String::from_utf8_lossy(&body), "Failed to parse Slack event payload");
            AppError::BadRequest(format!("Invalid event payload: {}", e))
        })?;

    {
        let span = tracing::Span::current();
        span.record("event_type", payload.event_type.as_str());
        if let Some(ref tid) = payload.team_id {
            span.record("team_id", tid.as_str());
        }
        if let Some(ref eid) = payload.event_id {
            span.record("event_id", eid.as_str());
        }
        if let Some(ref evt) = payload.event {
            span.record("inner_event_type", evt.event_type.as_str());
            if let Some(ref sub) = evt.subtype {
                span.record("inner_subtype", sub.as_str());
            }
        }
    }
    info!("Slack event received");

    // Deduplicate events using event_id in Redis. Slack retries events when it
    // doesn't get a fast 200; we accept retries but skip already-processed events
    // instead of blindly dropping all retries (which loses events that errored on
    // the first attempt).
    if let Some(ref event_id) = payload.event_id {
        let dedup_key = format!("slack:event_dedup:{}", event_id);
        let mut conn = state
            .redis
            .get()
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis: {}", e)))?;
        let is_new: bool = conn.set_nx(&dedup_key, "1").await.unwrap_or(false);
        if !is_new {
            info!(event_id = %event_id, "Duplicate Slack event, skipping");
            return Ok(StatusCode::OK.into_response());
        }
        // Expire after 5 minutes — well beyond Slack's retry window
        let _: () = conn.expire(&dedup_key, 300).await.unwrap_or(());
    }

    // URL verification challenge
    if payload.event_type == "url_verification" {
        if let Some(challenge) = payload.challenge {
            return Ok(Json(serde_json::json!({ "challenge": challenge })).into_response());
        }
    }

    // Event callbacks
    if payload.event_type == "event_callback" {
        if let Some(ref event) = payload.event {
            let team_id = payload.team_id.clone().unwrap_or_default();

            match event.event_type.as_str() {
                "app_uninstalled" => {
                    handle_app_uninstalled(&state, &team_id).await;
                }
                "tokens_revoked" => {
                    handle_tokens_revoked(&state, &team_id).await;
                }
                "app_mention" => {
                    let state_clone = state.clone();
                    let team_id = team_id.clone();
                    let user = event.user.clone().unwrap_or_default();
                    let text = event.text.clone().unwrap_or_default();
                    let channel = event.channel.clone().unwrap_or_default();
                    let ts = event.ts.clone().unwrap_or_default();
                    let thread_ts = event.thread_ts.clone().unwrap_or_else(|| ts.clone());

                    let span =
                        info_span!("slack.mention", %team_id, slack_user_id = %user, %channel);
                    tokio::spawn(
                        async move {
                            if let Err(e) = handle_app_mention(
                                &state_clone,
                                &team_id,
                                &user,
                                &text,
                                &channel,
                                &ts,
                                &thread_ts,
                            )
                            .await
                            {
                                error!(error = %e, "Failed to handle Slack app_mention");
                            }
                        }
                        .instrument(span),
                    );
                }
                "message" => {
                    // Slack's Assistant API sometimes delivers user messages as
                    // `message_changed` subtypes where the real user/text are in
                    // a nested `message` object. Resolve the effective fields.
                    let subtype = event.subtype.as_deref();
                    let resolved = match subtype {
                        Some("message_changed") => event.message.as_ref().map(|inner| {
                            (
                                inner.user.clone().unwrap_or_default(),
                                inner.text.clone().unwrap_or_default(),
                                inner.bot_id.clone(),
                                inner.thread_ts.clone().or_else(|| event.thread_ts.clone()),
                            )
                        }),
                        Some(
                            "message_deleted" | "channel_join" | "channel_leave" | "bot_message",
                        ) => None,
                        _ => Some((
                            event.user.clone().unwrap_or_default(),
                            event.text.clone().unwrap_or_default(),
                            event.bot_id.clone(),
                            event.thread_ts.clone(),
                        )),
                    };

                    if let Some((eff_user, eff_text, eff_bot_id, eff_thread_ts)) = resolved {
                        if eff_bot_id.is_some() {
                            // Ignore bot's own messages to avoid loops
                        } else if event.channel_type.as_deref() == Some("im") {
                            let state_clone = state.clone();
                            let team_id = team_id.clone();
                            let channel = event.channel.clone().unwrap_or_default();
                            let ts = event.ts.clone().unwrap_or_default();
                            let thread_ts = eff_thread_ts.unwrap_or_else(|| ts.clone());

                            let span = info_span!("slack.assistant.dm", %team_id, slack_user_id = %eff_user, %channel);
                            tokio::spawn(
                                async move {
                                    if let Err(e) = handle_assistant_dm(
                                        &state_clone,
                                        &team_id,
                                        &eff_user,
                                        &eff_text,
                                        &channel,
                                        &thread_ts,
                                    )
                                    .await
                                    {
                                        error!(error = %e, "Failed to handle Slack assistant DM");
                                    }
                                }
                                .instrument(span),
                            );
                        } else {
                            info!(
                                channel_type = ?event.channel_type,
                                subtype = ?subtype,
                                channel = ?event.channel,
                                "Ignoring message event (not an IM)"
                            );
                        }
                    }
                }
                "assistant_thread_started" => {
                    if let Some(ref at) = event.assistant_thread {
                        if let (Some(channel_id), Some(thread_ts)) = (&at.channel_id, &at.thread_ts)
                        {
                            let state_clone = state.clone();
                            let team_id = team_id.clone();
                            let channel_id = channel_id.clone();
                            let thread_ts = thread_ts.clone();

                            let span =
                                info_span!("slack.assistant.thread_started", %team_id, %channel_id);
                            tokio::spawn(async move {
                                if let Err(e) = handle_assistant_thread_started(
                                    &state_clone, &team_id, &channel_id, &thread_ts,
                                ).await {
                                    error!(error = %e, "Failed to handle assistant_thread_started");
                                }
                            }.instrument(span));
                        } else {
                            warn!("assistant_thread_started missing channel_id or thread_ts");
                        }
                    }
                }
                other => {
                    info!(event_type = %other, "Ignoring unhandled Slack event");
                }
            }
        }
    }

    Ok(StatusCode::OK.into_response())
}

// ─── Interactivity (Block Kit buttons, modal submissions) ────────────────────

/// POST /slack/interactivity — handles button clicks and modal submissions.
///
/// Slack sends a `application/x-www-form-urlencoded` body with a single `payload`
/// field containing the JSON interaction payload.
#[tracing::instrument(name = "slack.interactivity", skip_all, fields(interaction_type))]
async fn handle_interactivity(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    // Verify signing secret
    if let Some(ref secret) = state.config.slack_signing_secret {
        let timestamp = headers
            .get("X-Slack-Request-Timestamp")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let signature = headers
            .get("X-Slack-Signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !verify_slack_signature(secret, timestamp, &body, signature) {
            return Err(AppError::Forbidden("Invalid Slack signature".into()));
        }
    }

    let body_str = String::from_utf8_lossy(&body);
    let payload_json = body_str
        .strip_prefix("payload=")
        .ok_or_else(|| AppError::BadRequest("Missing payload field".into()))?;
    let payload_decoded = urlencoding::decode(payload_json)
        .map_err(|e| AppError::BadRequest(format!("URL decode error: {}", e)))?;
    let payload: serde_json::Value = serde_json::from_str(&payload_decoded)
        .map_err(|e| AppError::BadRequest(format!("Invalid JSON payload: {}", e)))?;

    let interaction_type = payload["type"].as_str().unwrap_or("");
    tracing::Span::current().record("interaction_type", interaction_type);

    match interaction_type {
        "block_actions" => {
            handle_block_action(&state, &payload).await?;
            Ok(StatusCode::OK.into_response())
        }
        "view_submission" => {
            handle_view_submission(&state, &payload).await?;
            // Return empty 200 to close the modal
            Ok(StatusCode::OK.into_response())
        }
        other => {
            info!(interaction_type = %other, "Ignoring unhandled Slack interaction type");
            Ok(StatusCode::OK.into_response())
        }
    }
}

/// Handle block_actions: user clicked the "Deposit Secret" button.
#[tracing::instrument(name = "slack.interactivity.block_action", skip_all)]
async fn handle_block_action(
    state: &Arc<WatchState>,
    payload: &serde_json::Value,
) -> anyhow::Result<()> {
    let actions = payload["actions"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("No actions in payload"))?;

    for action in actions {
        let action_id = action["action_id"].as_str().unwrap_or("");

        if action_id == "deposit_secret" {
            handle_deposit_secret_action(state, payload, action).await?;
        } else if action_id.starts_with("select_project:") {
            handle_select_project_action(state, payload, action).await?;
        }
    }

    Ok(())
}

/// Handle "Deposit Secret" button click → open modal.
#[tracing::instrument(name = "slack.interactivity.deposit_secret", skip_all)]
async fn handle_deposit_secret_action(
    state: &Arc<WatchState>,
    payload: &serde_json::Value,
    action: &serde_json::Value,
) -> anyhow::Result<()> {
    let trigger_id = payload["trigger_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No trigger_id in payload"))?;

    let metadata_str = action["value"].as_str().unwrap_or("{}");
    let metadata: serde_json::Value = serde_json::from_str(metadata_str).unwrap_or_default();

    let team_id = payload["team"]["id"].as_str().unwrap_or("");
    let slack_user_id = payload["user"]["id"].as_str().unwrap_or("");

    let user_mapping = sqlx::query_as::<_, (Uuid,)>(
        "SELECT user_id FROM slack_user_mappings WHERE team_id = $1 AND slack_user_id = $2",
    )
    .bind(team_id)
    .bind(slack_user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    if user_mapping.is_none() {
        warn!(%team_id, %slack_user_id, "Unlinked user tried to deposit secret");
        return Ok(());
    }

    let (_, config) = resolve_project_from_team(state, team_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("No Slack integration for team {}", team_id))?;
    let bot_token = decrypt_bot_token(state, &config)
        .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

    let private_metadata = serde_json::json!({
        "slot_id": metadata["slot_id"],
        "project_id": metadata["project_id"],
        "channel": metadata["channel"],
        "thread_ts": metadata["thread_ts"],
        "team_id": team_id,
        "slack_user_id": slack_user_id,
    })
    .to_string();

    let modal = serde_json::json!({
        "type": "modal",
        "callback_id": "deposit_secret_modal",
        "private_metadata": private_metadata,
        "title": { "type": "plain_text", "text": "Deposit Secret" },
        "submit": { "type": "plain_text", "text": "Submit" },
        "close": { "type": "plain_text", "text": "Cancel" },
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":lock: This value will be encrypted and stored securely. The AI agent will never see it."
                }
            },
            {
                "type": "input",
                "block_id": "secret_input_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "secret_value",
                    "placeholder": { "type": "plain_text", "text": "Paste your secret here" }
                },
                "label": { "type": "plain_text", "text": "Secret Value" }
            }
        ]
    });

    let http = reqwest::Client::new();
    let resp = http
        .post("https://slack.com/api/views.open")
        .bearer_auth(&bot_token)
        .json(&serde_json::json!({
            "trigger_id": trigger_id,
            "view": modal,
        }))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    if data["ok"].as_bool() != Some(true) {
        error!(error = ?data["error"], "views.open failed");
    }

    Ok(())
}

/// Handle project selection button click → store channel→project mapping.
#[tracing::instrument(name = "slack.interactivity.select_project", skip_all)]
async fn handle_select_project_action(
    state: &Arc<WatchState>,
    payload: &serde_json::Value,
    action: &serde_json::Value,
) -> anyhow::Result<()> {
    let metadata_str = action["value"].as_str().unwrap_or("{}");
    let metadata: serde_json::Value = serde_json::from_str(metadata_str).unwrap_or_default();

    let project_id_str = metadata["project_id"].as_str().unwrap_or("");
    let team_id = metadata["team_id"].as_str().unwrap_or("");
    let channel = metadata["channel"].as_str().unwrap_or("");
    let slack_user_id = payload["user"]["id"].as_str().unwrap_or("");

    let project_id: Uuid = project_id_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid project_id in select_project action"))?;

    set_channel_project(state, team_id, channel, project_id, slack_user_id).await?;

    // Look up the project name for the confirmation message
    let project_name = sqlx::query_scalar::<_, String>("SELECT name FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(state.db.as_ref())
        .await?
        .unwrap_or_else(|| project_id_str.to_string());

    // Get bot token to post confirmation
    let (_, config) = resolve_project_from_team(state, team_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("No Slack integration for team {}", team_id))?;
    let bot_token = decrypt_bot_token(state, &config)
        .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

    let http = reqwest::Client::new();

    // Update the picker message to show the selection
    let msg_channel = payload["channel"]["id"].as_str().unwrap_or(channel);
    let msg_ts = payload["message"]["ts"].as_str().unwrap_or("");
    if !msg_ts.is_empty() {
        let _ = slack_update_message(
            &http, &bot_token, msg_channel, msg_ts,
            &format!(":white_check_mark: This channel is now using the *{}* project. You can @mention me again to start.", project_name),
        ).await;
    }

    info!(%team_id, %channel, %project_id, %slack_user_id, "Channel→project mapping set");

    Ok(())
}

/// Handle view_submission: user submitted the secret deposit modal.
#[tracing::instrument(name = "slack.interactivity.view_submission", skip_all)]
async fn handle_view_submission(
    state: &Arc<WatchState>,
    payload: &serde_json::Value,
) -> anyhow::Result<()> {
    let callback_id = payload["view"]["callback_id"].as_str().unwrap_or("");
    if callback_id != "deposit_secret_modal" {
        return Ok(());
    }

    let private_metadata_str = payload["view"]["private_metadata"].as_str().unwrap_or("{}");
    let meta: serde_json::Value = serde_json::from_str(private_metadata_str).unwrap_or_default();

    let slot_id = meta["slot_id"].as_str().unwrap_or("");
    let project_id = meta["project_id"].as_str().unwrap_or("");
    let channel = meta["channel"].as_str().unwrap_or("");
    let thread_ts = meta["thread_ts"].as_str().unwrap_or("");
    let team_id = meta["team_id"].as_str().unwrap_or("");
    let slack_user_id = meta["slack_user_id"].as_str().unwrap_or("");

    if slot_id.is_empty() || project_id.is_empty() {
        warn!("Secret deposit modal submitted with missing slot_id or project_id");
        return Ok(());
    }

    // Resolve Reiver user_id from Slack user
    let user_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM slack_user_mappings WHERE team_id = $1 AND slack_user_id = $2",
    )
    .bind(team_id)
    .bind(slack_user_id)
    .fetch_optional(state.db.as_ref())
    .await?
    .ok_or_else(|| anyhow::anyhow!("No user mapping for {}/{}", team_id, slack_user_id))?;

    // Extract secret value from modal state
    let secret_value = payload["view"]["state"]["values"]["secret_input_block"]["secret_value"]
        ["value"]
        .as_str()
        .unwrap_or("");

    if secret_value.is_empty() {
        warn!("Empty secret submitted via Slack modal");
        return Ok(());
    }

    // Call Flow's deposit endpoint
    let flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());

    let deposit_url = format!("{}/api/secrets/deposit/{}", flow_url, slot_id);
    let http = reqwest::Client::new();
    let resp = http
        .post(&deposit_url)
        .header("X-Project-Id", project_id)
        .header("X-User-Id", user_id.to_string())
        .json(&serde_json::json!({ "value": secret_value }))
        .send()
        .await?;

    let success = resp.status().is_success();

    // Update the original Slack message to show deposit result
    if !channel.is_empty() && !thread_ts.is_empty() {
        let (_, config) = resolve_project_from_team(state, team_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("No Slack integration for team {}", team_id))?;
        let bot_token = decrypt_bot_token(state, &config)
            .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

        let status_msg = if success {
            ":white_check_mark: Secret deposited successfully.".to_string()
        } else {
            let err_body: serde_json::Value = resp.json().await.unwrap_or_default();
            let detail = err_body["error"].as_str().unwrap_or("unknown error");
            format!(":x: Failed to deposit secret: {}", detail)
        };

        let _ = slack_post_message(&http, &bot_token, channel, &status_msg, Some(thread_ts)).await;
    }

    Ok(())
}

#[tracing::instrument(name = "slack.app_uninstalled", skip(state))]
async fn handle_app_uninstalled(state: &Arc<WatchState>, team_id: &str) {
    // Disable all Slack integrations for this team
    let result = sqlx::query(
        "UPDATE notification_channels SET enabled = false, updated_at = NOW() WHERE channel_type = $1 AND config->>'team_id' = $2",
    )
    .bind(CHANNEL_TYPE)
    .bind(team_id)
    .execute(state.db.as_ref())
    .await;

    match result {
        Ok(r) => {
            info!(%team_id, rows = r.rows_affected(), "Slack app uninstalled, integrations disabled")
        }
        Err(e) => error!(%team_id, error = %e, "Failed to disable Slack integrations on uninstall"),
    }

    // Clean up user mappings
    let _ = sqlx::query("DELETE FROM slack_user_mappings WHERE team_id = $1")
        .bind(team_id)
        .execute(state.db.as_ref())
        .await;

    // Clean up channel→project mappings
    let _ = sqlx::query("DELETE FROM slack_channel_project_mappings WHERE team_id = $1")
        .bind(team_id)
        .execute(state.db.as_ref())
        .await;
}

/// Handle `tokens_revoked`: Slack rotated or revoked specific tokens.
///
/// Unlike `app_uninstalled`, this does NOT mean the app was removed. Token
/// rotation is routine. We log the event but preserve user mappings and
/// channel preferences -- the bot token stored in `notification_channels`
/// remains valid (it's the *installed* bot token, not a user OAuth token).
#[tracing::instrument(name = "slack.tokens_revoked", skip(state))]
async fn handle_tokens_revoked(state: &Arc<WatchState>, team_id: &str) {
    warn!(%team_id, "Slack tokens_revoked event received — user/channel mappings preserved");

    // If the bot token itself was rotated, the next API call will fail and
    // the user will need to re-install. But we don't proactively wipe data
    // because token rotation alone should not break user account links.
}

// ─── Assistant View ──────────────────────────────────────────────────────────

/// Handle `assistant_thread_started`: set suggested prompts in the new thread.
#[tracing::instrument(name = "slack.assistant.thread_started", skip(state))]
async fn handle_assistant_thread_started(
    state: &Arc<WatchState>,
    team_id: &str,
    channel_id: &str,
    thread_ts: &str,
) -> anyhow::Result<()> {
    let (_, config) = resolve_project_from_team(state, team_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("No Slack integration for team {}", team_id))?;
    let bot_token = decrypt_bot_token(state, &config)
        .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

    let http = reqwest::Client::new();

    // Set suggested prompts for the new assistant thread
    let resp = http
        .post("https://slack.com/api/assistant.threads.setSuggestedPrompts")
        .bearer_auth(&bot_token)
        .json(&serde_json::json!({
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "title": "What can I help you with?",
            "prompts": [
                {
                    "title": "Production issues",
                    "message": "What's broken in production right now? Show me recent exceptions and any firing alerts."
                },
                {
                    "title": "Slowest endpoints",
                    "message": "Show me the slowest API endpoints in the last hour with their p95 latencies."
                },
                {
                    "title": "LLM spending",
                    "message": "How much did we spend on LLM requests today? Break it down by model."
                },
                {
                    "title": "Service health",
                    "message": "Give me an overview of all services — error rates, request counts, and average latencies."
                }
            ]
        }))
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    if data["ok"].as_bool() != Some(true) {
        warn!(error = ?data["error"], "assistant.threads.setSuggestedPrompts failed");
    }

    Ok(())
}

/// Handle a DM to the bot (message.im) from the Assistant side panel.
///
/// Works like `handle_app_mention` but without needing to strip a @mention prefix.
/// DMs are always in a 1:1 channel, so there's no channel-level project ambiguity
/// — we use the user's linked project directly.
#[tracing::instrument(name = "slack.assistant.dm", skip(state, text))]
async fn handle_assistant_dm(
    state: &Arc<WatchState>,
    team_id: &str,
    slack_user_id: &str,
    text: &str,
    channel: &str,
    thread_ts: &str,
) -> anyhow::Result<()> {
    let (_, config) = resolve_project_from_team(state, team_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("No Slack integration for team {}", team_id))?;
    let bot_token = decrypt_bot_token(state, &config)
        .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

    let http = reqwest::Client::new();

    // Resolve Reiver user
    let user_mapping = sqlx::query_as::<_, (Uuid,)>(
        "SELECT user_id FROM slack_user_mappings WHERE team_id = $1 AND slack_user_id = $2",
    )
    .bind(team_id)
    .bind(slack_user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let user_id = match user_mapping {
        Some((uid,)) => uid,
        None => {
            let base_url = state
                .config
                .api_base_url
                .as_deref()
                .unwrap_or("http://localhost:3000");
            let token = generate_link_token(
                state.config.slack_signing_secret.as_deref().unwrap_or(""),
                team_id,
                slack_user_id,
            );
            let link_url = format!(
                "{}/api/slack/link?team_id={}&slack_user_id={}&token={}",
                base_url, team_id, slack_user_id, token
            );
            let msg = format!(
                "To use Moodeng, please connect your Reiver account first:\n<{}|Link Account>",
                link_url
            );
            slack_post_message(&http, &bot_token, channel, &msg, Some(thread_ts)).await?;
            return Ok(());
        }
    };

    if text.trim().is_empty() {
        return Ok(());
    }

    // Set "is thinking..." status in the assistant thread
    let _ = http
        .post("https://slack.com/api/assistant.threads.setStatus")
        .bearer_auth(&bot_token)
        .json(&serde_json::json!({
            "channel_id": channel,
            "thread_ts": thread_ts,
            "status": "is thinking...",
        }))
        .send()
        .await;

    // Resolve project — for DMs, if user has multiple projects, pick the first.
    // The user mapping already implies a project context.
    let all_projects = resolve_all_projects_from_team(state, team_id).await;
    let project_id = if all_projects.len() == 1 {
        all_projects[0].0
    } else {
        // In DMs, check the user mapping's project_id
        sqlx::query_scalar::<_, Uuid>(
            "SELECT project_id FROM slack_user_mappings WHERE team_id = $1 AND slack_user_id = $2",
        )
        .bind(team_id)
        .bind(slack_user_id)
        .fetch_optional(state.db.as_ref())
        .await?
        .unwrap_or(all_projects.first().map(|p| p.0).unwrap_or_default())
    };

    let conversation_id =
        resolve_or_create_conversation(state, project_id, user_id, team_id, channel, thread_ts)
            .await?;

    let flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());

    let agent_url = format!("{}/api/agent/chat", flow_url);
    let agent_payload = serde_json::json!({
        "conversation_id": conversation_id,
        "message": text,
    });

    let agent_resp = http
        .post(&agent_url)
        .header("X-Project-Id", project_id.to_string())
        .header("X-User-Id", user_id.to_string())
        .header("Accept", "text/event-stream")
        .json(&agent_payload)
        .send()
        .await;

    let mut full_text = String::new();
    let mut reply_ts: Option<String> = None;
    let mut last_update = std::time::Instant::now();
    let mut sse_buffer = String::new();

    match agent_resp {
        Ok(resp) => {
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;

            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(_) => break,
                };
                sse_buffer.push_str(&String::from_utf8_lossy(&chunk));

                while let Some(newline_pos) = sse_buffer.find('\n') {
                    let line = sse_buffer[..newline_pos].trim_end_matches('\r').to_string();
                    sse_buffer = sse_buffer[newline_pos + 1..].to_string();

                    if let Some(data) = line.strip_prefix("data: ") {
                        if let Ok(evt) = serde_json::from_str::<serde_json::Value>(data) {
                            let evt_type = evt["type"].as_str().unwrap_or("");
                            match evt_type {
                                "text_delta" => {
                                    if let Some(content) = evt["content"].as_str() {
                                        full_text.push_str(content);

                                        if last_update.elapsed()
                                            >= std::time::Duration::from_secs(1)
                                        {
                                            match &reply_ts {
                                                None => {
                                                    match slack_post_message(
                                                        &http,
                                                        &bot_token,
                                                        channel,
                                                        &full_text,
                                                        Some(thread_ts),
                                                    )
                                                    .await
                                                    {
                                                        Ok(Some(ts)) => reply_ts = Some(ts),
                                                        Ok(None) => {}
                                                        Err(e) => warn!(
                                                            "Failed to post assistant reply: {}",
                                                            e
                                                        ),
                                                    }
                                                }
                                                Some(ts) => {
                                                    let _ = slack_update_message(
                                                        &http, &bot_token, channel, ts, &full_text,
                                                    )
                                                    .await;
                                                }
                                            }
                                            last_update = std::time::Instant::now();
                                        }
                                    }
                                }
                                "tool_start" => {
                                    if let Some(name) = evt["name"].as_str() {
                                        // Update assistant status to show tool activity
                                        let _ = http
                                            .post(
                                                "https://slack.com/api/assistant.threads.setStatus",
                                            )
                                            .bearer_auth(&bot_token)
                                            .json(&serde_json::json!({
                                                "channel_id": channel,
                                                "thread_ts": thread_ts,
                                                "status": format!("is running {}...", name),
                                            }))
                                            .send()
                                            .await;
                                    }
                                }
                                "tool_result" => {
                                    if evt["name"].as_str() == Some("create_secret_slot") {
                                        if let Some(output) = evt.get("output") {
                                            let slot_id = output["slot_id"].as_str().unwrap_or("");
                                            let purpose =
                                                output["purpose"].as_str().unwrap_or("secret");
                                            if !slot_id.is_empty() {
                                                let metadata = serde_json::json!({
                                                    "slot_id": slot_id,
                                                    "project_id": project_id.to_string(),
                                                    "channel": channel,
                                                    "thread_ts": thread_ts,
                                                })
                                                .to_string();
                                                let blocks = serde_json::json!([
                                                    {
                                                        "type": "section",
                                                        "text": {
                                                            "type": "mrkdwn",
                                                            "text": format!(":lock: *Secure deposit requested:* {}\nThis value will not be visible to the AI agent.", purpose)
                                                        }
                                                    },
                                                    {
                                                        "type": "actions",
                                                        "elements": [{
                                                            "type": "button",
                                                            "text": { "type": "plain_text", "text": "Deposit Secret" },
                                                            "style": "primary",
                                                            "action_id": "deposit_secret",
                                                            "value": metadata,
                                                        }]
                                                    }
                                                ]);
                                                let body = serde_json::json!({
                                                    "channel": channel,
                                                    "text": format!("Secure deposit requested: {}", purpose),
                                                    "blocks": blocks,
                                                    "thread_ts": thread_ts,
                                                });
                                                let _ = http
                                                    .post("https://slack.com/api/chat.postMessage")
                                                    .bearer_auth(&bot_token)
                                                    .json(&body)
                                                    .send()
                                                    .await;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            full_text = format!("Sorry, I encountered an error: {}", e);
        }
    }

    // Final reply
    if full_text.is_empty() {
        full_text = "I wasn't able to generate a response.".into();
    }

    match &reply_ts {
        None => {
            slack_post_message(&http, &bot_token, channel, &full_text, Some(thread_ts)).await?;
        }
        Some(ts) => {
            let _ = slack_update_message(&http, &bot_token, channel, ts, &full_text).await;
        }
    }

    // Status auto-clears when the bot replies, but clear explicitly just in case
    let _ = http
        .post("https://slack.com/api/assistant.threads.setStatus")
        .bearer_auth(&bot_token)
        .json(&serde_json::json!({
            "channel_id": channel,
            "thread_ts": thread_ts,
            "status": "",
        }))
        .send()
        .await;

    Ok(())
}

// ─── Agent Bridge (app_mention → Moodeng) ────────────────────────────────────

/// Resolve a single project+config from a Slack team_id (picks first match).
/// Used by interactivity handlers where the project is already contextually known.
async fn resolve_project_from_team(
    state: &Arc<WatchState>,
    team_id: &str,
) -> Option<(Uuid, serde_json::Value)> {
    sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT project_id, config FROM notification_channels WHERE channel_type = $1 AND config->>'team_id' = $2 AND enabled = true LIMIT 1",
    )
    .bind(CHANNEL_TYPE)
    .bind(team_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten()
}

/// Resolve ALL projects connected to a Slack workspace.
async fn resolve_all_projects_from_team(
    state: &Arc<WatchState>,
    team_id: &str,
) -> Vec<(Uuid, serde_json::Value)> {
    sqlx::query_as::<_, (Uuid, serde_json::Value)>(
        "SELECT project_id, config FROM notification_channels WHERE channel_type = $1 AND config->>'team_id' = $2 AND enabled = true",
    )
    .bind(CHANNEL_TYPE)
    .bind(team_id)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default()
}

/// Look up a channel-level project preference (set when workspace has multiple projects).
async fn resolve_channel_project(
    state: &Arc<WatchState>,
    team_id: &str,
    channel_id: &str,
) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT project_id FROM slack_channel_project_mappings WHERE team_id = $1 AND channel_id = $2",
    )
    .bind(team_id)
    .bind(channel_id)
    .fetch_optional(state.db.as_ref())
    .await
    .ok()
    .flatten()
}

/// Store a channel→project preference.
async fn set_channel_project(
    state: &Arc<WatchState>,
    team_id: &str,
    channel_id: &str,
    project_id: Uuid,
    slack_user_id: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO slack_channel_project_mappings (team_id, channel_id, project_id, set_by_slack_user) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (team_id, channel_id) DO UPDATE SET project_id = $3, set_by_slack_user = $4",
    )
    .bind(team_id)
    .bind(channel_id)
    .bind(project_id)
    .bind(slack_user_id)
    .execute(state.db.as_ref())
    .await?;
    Ok(())
}

fn decrypt_bot_token(state: &Arc<WatchState>, config: &serde_json::Value) -> Option<String> {
    let encrypted = config["bot_token"].as_str()?;
    state.encryptor.decrypt(encrypted).ok()
}

/// Post a message to a Slack channel using the Bot API.
#[tracing::instrument(name = "slack.api.post_message", skip(http, bot_token, text))]
async fn slack_post_message(
    http: &reqwest::Client,
    bot_token: &str,
    channel: &str,
    text: &str,
    thread_ts: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let mut body = serde_json::json!({
        "channel": channel,
        "text": text,
    });
    if let Some(ts) = thread_ts {
        body["thread_ts"] = serde_json::Value::String(ts.into());
    }

    let resp = http
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    if data["ok"].as_bool() != Some(true) {
        anyhow::bail!(
            "chat.postMessage failed: {}",
            data["error"].as_str().unwrap_or("unknown")
        );
    }
    Ok(data["ts"].as_str().map(|s| s.to_string()))
}

#[tracing::instrument(name = "slack.api.update_message", skip(http, bot_token, text))]
async fn slack_update_message(
    http: &reqwest::Client,
    bot_token: &str,
    channel: &str,
    ts: &str,
    text: &str,
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "channel": channel,
        "ts": ts,
        "text": text,
    });
    let resp = http
        .post("https://slack.com/api/chat.update")
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    if data["ok"].as_bool() != Some(true) {
        anyhow::bail!(
            "chat.update failed: {}",
            data["error"].as_str().unwrap_or("unknown")
        );
    }
    Ok(())
}

#[tracing::instrument(name = "slack.api.add_reaction", skip(http, bot_token))]
async fn slack_add_reaction(
    http: &reqwest::Client,
    bot_token: &str,
    channel: &str,
    ts: &str,
    emoji: &str,
) -> anyhow::Result<()> {
    let body = serde_json::json!({ "channel": channel, "timestamp": ts, "name": emoji });
    let _ = http
        .post("https://slack.com/api/reactions.add")
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await?;
    Ok(())
}

#[tracing::instrument(name = "slack.api.remove_reaction", skip(http, bot_token))]
async fn slack_remove_reaction(
    http: &reqwest::Client,
    bot_token: &str,
    channel: &str,
    ts: &str,
    emoji: &str,
) -> anyhow::Result<()> {
    let body = serde_json::json!({ "channel": channel, "timestamp": ts, "name": emoji });
    let _ = http
        .post("https://slack.com/api/reactions.remove")
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await?;
    Ok(())
}

/// Strip `<@BOTID>` mention prefix from Slack message text.
fn strip_mention(text: &str) -> String {
    use std::sync::LazyLock;
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"<@[A-Z0-9]+>\s*").unwrap());
    RE.replace_all(text, "").trim().to_string()
}

/// Fetch project names for all integrations in a team (for the picker UI).
async fn all_projects_for_picker(
    state: &Arc<WatchState>,
    team_id: &str,
) -> Vec<(Uuid, String, serde_json::Value)> {
    sqlx::query_as::<_, (Uuid, String, serde_json::Value)>(
        "SELECT nc.project_id, p.name, nc.config \
         FROM notification_channels nc \
         JOIN projects p ON p.id = nc.project_id \
         WHERE nc.channel_type = $1 AND nc.config->>'team_id' = $2 AND nc.enabled = true",
    )
    .bind(CHANNEL_TYPE)
    .bind(team_id)
    .fetch_all(state.db.as_ref())
    .await
    .unwrap_or_default()
}

/// Post a project-picker message with one button per project.
async fn send_project_picker(
    state: &Arc<WatchState>,
    projects: &[(Uuid, String, serde_json::Value)],
    team_id: &str,
    channel: &str,
    thread_ts: &str,
) -> anyhow::Result<()> {
    if projects.is_empty() {
        anyhow::bail!("No projects available for team {}", team_id);
    }

    // Use the first project's bot token (all integrations for the same team share the same Slack app install)
    let bot_token = decrypt_bot_token(state, &projects[0].2)
        .ok_or_else(|| anyhow::anyhow!("Cannot decrypt bot token for team {}", team_id))?;

    let buttons: Vec<serde_json::Value> = projects
        .iter()
        .map(|(pid, name, _)| {
            serde_json::json!({
                "type": "button",
                "text": { "type": "plain_text", "text": name },
                "action_id": format!("select_project:{}", pid),
                "value": serde_json::json!({
                    "project_id": pid.to_string(),
                    "team_id": team_id,
                    "channel": channel,
                }).to_string(),
            })
        })
        .collect();

    let blocks = serde_json::json!([
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "This Slack workspace is connected to multiple projects. Which project should I use in this channel?"
            }
        },
        {
            "type": "actions",
            "elements": buttons,
        }
    ]);

    let http = reqwest::Client::new();
    let body = serde_json::json!({
        "channel": channel,
        "text": "Please select a project for this channel.",
        "blocks": blocks,
        "thread_ts": thread_ts,
    });
    let resp = http
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(&bot_token)
        .json(&body)
        .send()
        .await?;
    let data: serde_json::Value = resp.json().await?;
    if data["ok"].as_bool() != Some(true) {
        anyhow::bail!(
            "chat.postMessage failed: {}",
            data["error"].as_str().unwrap_or("unknown")
        );
    }
    Ok(())
}

/// Handle an @mention of Moodeng in a Slack channel.
#[tracing::instrument(name = "slack.mention", skip(state, raw_text))]
async fn handle_app_mention(
    state: &Arc<WatchState>,
    team_id: &str,
    slack_user_id: &str,
    raw_text: &str,
    channel: &str,
    msg_ts: &str,
    thread_ts: &str,
) -> anyhow::Result<()> {
    let all_projects = resolve_all_projects_from_team(state, team_id).await;
    if all_projects.is_empty() {
        anyhow::bail!("No Slack integration found for team {}", team_id);
    }

    let (project_id, config) = if all_projects.len() == 1 {
        all_projects.into_iter().next().unwrap()
    } else {
        // Multiple projects — check for a channel-level preference
        if let Some(channel_project_id) = resolve_channel_project(state, team_id, channel).await {
            match all_projects
                .into_iter()
                .find(|(pid, _)| *pid == channel_project_id)
            {
                Some(found) => found,
                None => {
                    // Preference points to a project that's no longer connected; clear it and re-prompt
                    let _ = sqlx::query("DELETE FROM slack_channel_project_mappings WHERE team_id = $1 AND channel_id = $2")
                        .bind(team_id).bind(channel).execute(state.db.as_ref()).await;
                    return send_project_picker(
                        state,
                        &all_projects_for_picker(state, team_id).await,
                        team_id,
                        channel,
                        thread_ts,
                    )
                    .await;
                }
            }
        } else {
            return send_project_picker(
                state,
                &all_projects_for_picker(state, team_id).await,
                team_id,
                channel,
                thread_ts,
            )
            .await;
        }
    };

    let bot_token = decrypt_bot_token(state, &config)
        .ok_or_else(|| anyhow::anyhow!("Failed to decrypt bot token for team {}", team_id))?;

    let http = reqwest::Client::new();

    // Resolve Reiver user from Slack user mapping
    let user_mapping = sqlx::query_as::<_, (Uuid,)>(
        "SELECT user_id FROM slack_user_mappings WHERE team_id = $1 AND slack_user_id = $2",
    )
    .bind(team_id)
    .bind(slack_user_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let user_id = match user_mapping {
        Some((uid,)) => uid,
        None => {
            // Send account linking prompt
            let base_url = state
                .config
                .api_base_url
                .as_deref()
                .unwrap_or("http://localhost:3000");
            let token = generate_link_token(
                state.config.slack_signing_secret.as_deref().unwrap_or(""),
                team_id,
                slack_user_id,
            );
            let link_url = format!(
                "{}/api/slack/link?team_id={}&slack_user_id={}&token={}",
                base_url, team_id, slack_user_id, token
            );
            let msg = format!(
                "To use Moodeng, please connect your Reiver account first:\n<{}|Link Account>",
                link_url
            );
            slack_post_message(&http, &bot_token, channel, &msg, Some(thread_ts)).await?;
            return Ok(());
        }
    };

    let message = strip_mention(raw_text);
    if message.is_empty() {
        slack_post_message(
            &http,
            &bot_token,
            channel,
            "How can I help?",
            Some(thread_ts),
        )
        .await?;
        return Ok(());
    }

    // Add thinking indicator
    let _ = slack_add_reaction(&http, &bot_token, channel, msg_ts, "hourglass").await;

    // Resolve or create conversation from thread
    let conversation_id =
        resolve_or_create_conversation(state, project_id, user_id, team_id, channel, thread_ts)
            .await?;

    // Call Flow agent via internal HTTP (SSE stream)
    let flow_url = std::env::var("FLOW_GATEWAY_URL")
        .or_else(|_| std::env::var("FLOW_URL"))
        .unwrap_or_else(|_| "http://localhost:3001".into());

    let agent_url = format!("{}/api/agent/chat", flow_url);
    let agent_payload = serde_json::json!({
        "conversation_id": conversation_id,
        "message": message,
    });

    let agent_resp = http
        .post(&agent_url)
        .header("X-Project-Id", project_id.to_string())
        .header("X-User-Id", user_id.to_string())
        .header("Accept", "text/event-stream")
        .json(&agent_payload)
        .send()
        .await;

    let mut full_text = String::new();
    let mut reply_ts: Option<String> = None;
    let mut last_update = std::time::Instant::now();
    let mut sse_buffer = String::new();

    match agent_resp {
        Ok(resp) => {
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;

            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(_) => break,
                };
                sse_buffer.push_str(&String::from_utf8_lossy(&chunk));

                // Process complete lines from the buffer
                while let Some(newline_pos) = sse_buffer.find('\n') {
                    let line = sse_buffer[..newline_pos].trim_end_matches('\r').to_string();
                    sse_buffer = sse_buffer[newline_pos + 1..].to_string();

                    if let Some(data) = line.strip_prefix("data: ") {
                        if let Ok(evt) = serde_json::from_str::<serde_json::Value>(data) {
                            let evt_type = evt["type"].as_str().unwrap_or("");
                            match evt_type {
                                "text_delta" => {
                                    if let Some(content) = evt["content"].as_str() {
                                        full_text.push_str(content);

                                        // Throttle Slack updates to ~1/sec
                                        if last_update.elapsed()
                                            >= std::time::Duration::from_secs(1)
                                        {
                                            match &reply_ts {
                                                None => {
                                                    match slack_post_message(
                                                        &http, &bot_token, channel, &full_text, Some(thread_ts),
                                                    ).await {
                                                        Ok(Some(ts)) => reply_ts = Some(ts),
                                                        Ok(None) => {}
                                                        Err(e) => warn!("Failed to post initial Slack message: {}", e),
                                                    }
                                                }
                                                Some(ts) => {
                                                    let _ = slack_update_message(
                                                        &http, &bot_token, channel, ts, &full_text,
                                                    )
                                                    .await;
                                                }
                                            }
                                            last_update = std::time::Instant::now();
                                        }
                                    }
                                }
                                "tool_start" => {
                                    if let Some(name) = evt["name"].as_str() {
                                        let status =
                                            format!("{}\n_Running {}..._", &full_text, name);
                                        if let Some(ref ts) = reply_ts {
                                            let _ = slack_update_message(
                                                &http, &bot_token, channel, ts, &status,
                                            )
                                            .await;
                                        }
                                    }
                                }
                                "tool_result" => {
                                    if evt["name"].as_str() == Some("create_secret_slot") {
                                        if let Some(output) = evt.get("output") {
                                            let slot_id = output["slot_id"].as_str().unwrap_or("");
                                            let purpose =
                                                output["purpose"].as_str().unwrap_or("secret");
                                            if !slot_id.is_empty() {
                                                let metadata = serde_json::json!({
                                                    "slot_id": slot_id,
                                                    "project_id": project_id.to_string(),
                                                    "channel": channel,
                                                    "thread_ts": thread_ts,
                                                })
                                                .to_string();
                                                let blocks = serde_json::json!([
                                                    {
                                                        "type": "section",
                                                        "text": {
                                                            "type": "mrkdwn",
                                                            "text": format!(":lock: *Secure deposit requested:* {}\nThis value will not be visible to the AI agent.", purpose)
                                                        }
                                                    },
                                                    {
                                                        "type": "actions",
                                                        "elements": [{
                                                            "type": "button",
                                                            "text": { "type": "plain_text", "text": "Deposit Secret" },
                                                            "style": "primary",
                                                            "action_id": "deposit_secret",
                                                            "value": metadata,
                                                        }]
                                                    }
                                                ]);
                                                let fallback = format!("Secure deposit requested: {}. Use the button to deposit your secret.", purpose);
                                                let body = serde_json::json!({
                                                    "channel": channel,
                                                    "text": fallback,
                                                    "blocks": blocks,
                                                    "thread_ts": thread_ts,
                                                });
                                                let resp = http
                                                    .post("https://slack.com/api/chat.postMessage")
                                                    .bearer_auth(&bot_token)
                                                    .json(&body)
                                                    .send()
                                                    .await;
                                                if let Err(e) = resp {
                                                    warn!(
                                                        "Failed to post secret deposit button: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            full_text = format!("Sorry, I encountered an error: {}", e);
        }
    }

    // Final update with complete text
    if full_text.is_empty() {
        full_text = "I wasn't able to generate a response.".into();
    }

    match &reply_ts {
        None => {
            slack_post_message(&http, &bot_token, channel, &full_text, Some(thread_ts)).await?;
        }
        Some(ts) => {
            let _ = slack_update_message(&http, &bot_token, channel, ts, &full_text).await;
        }
    }

    // Remove thinking indicator
    let _ = slack_remove_reaction(&http, &bot_token, channel, msg_ts, "hourglass").await;

    Ok(())
}

async fn resolve_or_create_conversation(
    state: &Arc<WatchState>,
    project_id: Uuid,
    user_id: Uuid,
    team_id: &str,
    channel_id: &str,
    thread_ts: &str,
) -> anyhow::Result<Uuid> {
    // Check for existing mapping
    let existing = sqlx::query_as::<_, (Uuid,)>(
        "SELECT conversation_id FROM slack_thread_conversations WHERE team_id = $1 AND channel_id = $2 AND thread_ts = $3",
    )
    .bind(team_id)
    .bind(channel_id)
    .bind(thread_ts)
    .fetch_optional(state.db.as_ref())
    .await?;

    if let Some((conv_id,)) = existing {
        return Ok(conv_id);
    }

    // Create a new agent conversation
    let title = format!("Slack thread {}", &thread_ts[..thread_ts.len().min(10)]);
    let (conv_id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO agent_conversations (project_id, user_id, title) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(project_id)
    .bind(user_id)
    .bind(&title)
    .fetch_one(state.db.as_ref())
    .await?;

    // Store the mapping
    sqlx::query(
        "INSERT INTO slack_thread_conversations (project_id, team_id, channel_id, thread_ts, conversation_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
    )
    .bind(project_id)
    .bind(team_id)
    .bind(channel_id)
    .bind(thread_ts)
    .bind(conv_id)
    .execute(state.db.as_ref())
    .await?;

    Ok(conv_id)
}

// ─── Account Linking ─────────────────────────────────────────────────────────

fn generate_link_token(signing_secret: &str, team_id: &str, slack_user_id: &str) -> String {
    let expiry = chrono::Utc::now().timestamp() + LINK_TOKEN_TTL_SECONDS as i64;
    let payload = format!("{}:{}:{}", team_id, slack_user_id, expiry);
    let mut mac = HmacSha256::new_from_slice(signing_secret.as_bytes())
        .unwrap_or_else(|_| HmacSha256::new_from_slice(b"fallback-key").unwrap());
    mac.update(payload.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    format!(
        "{}.{}",
        base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            payload.as_bytes()
        ),
        sig
    )
}

fn verify_link_token(signing_secret: &str, token: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = token.splitn(2, '.').collect();
    if parts.len() != 2 {
        return None;
    }
    let payload_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, parts[0]).ok()?;
    let payload = String::from_utf8(payload_bytes).ok()?;
    let fields: Vec<&str> = payload.splitn(3, ':').collect();
    if fields.len() != 3 {
        return None;
    }

    let expiry: i64 = fields[2].parse().ok()?;
    if chrono::Utc::now().timestamp() > expiry {
        return None;
    }

    let mut mac = HmacSha256::new_from_slice(signing_secret.as_bytes())
        .unwrap_or_else(|_| HmacSha256::new_from_slice(b"fallback-key").unwrap());
    mac.update(payload.as_bytes());
    let expected_sig = hex::encode(mac.finalize().into_bytes());
    if expected_sig != parts[1] {
        return None;
    }

    Some((fields[0].to_string(), fields[1].to_string()))
}

#[derive(Deserialize)]
struct LinkQuery {
    team_id: String,
    slack_user_id: String,
    token: String,
}

/// GET /slack/link — links a Slack user to their Reiver account.
#[tracing::instrument(name = "slack.link_account", skip_all, fields(team_id = %query.team_id, slack_user_id = %query.slack_user_id))]
async fn link_account(
    State(state): State<Arc<WatchState>>,
    headers: HeaderMap,
    Query(query): Query<LinkQuery>,
) -> Result<Response> {
    let user_id = crate::api::extract_user_id(&headers)?;
    let signing_secret = state.config.slack_signing_secret.as_deref().unwrap_or("");

    let (token_team, token_user) = verify_link_token(signing_secret, &query.token)
        .ok_or_else(|| AppError::BadRequest("Invalid or expired link token".into()))?;

    if token_team != query.team_id || token_user != query.slack_user_id {
        return Err(AppError::BadRequest("Token mismatch".into()));
    }

    // Resolve project from team
    let project_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT project_id FROM notification_channels WHERE channel_type = $1 AND config->>'team_id' = $2 AND enabled = true LIMIT 1",
    )
    .bind(CHANNEL_TYPE)
    .bind(&query.team_id)
    .fetch_optional(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB: {}", e)))?
    .ok_or_else(|| AppError::NotFound("No Slack integration found for this workspace".into()))?;

    sqlx::query(
        "INSERT INTO slack_user_mappings (project_id, team_id, slack_user_id, user_id) VALUES ($1, $2, $3, $4) ON CONFLICT (team_id, slack_user_id) DO UPDATE SET user_id = $4",
    )
    .bind(project_id)
    .bind(&query.team_id)
    .bind(&query.slack_user_id)
    .bind(user_id)
    .execute(state.db.as_ref())
    .await
    .map_err(|e| AppError::Internal(anyhow::anyhow!("DB: {}", e)))?;

    info!(%user_id, team_id = %query.team_id, slack_user = %query.slack_user_id, "Slack account linked");

    Ok(axum::response::Html(
        r#"<!DOCTYPE html><html><head><title>Account Linked</title></head>
        <body style="font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0">
        <div style="text-align:center"><h1>Account Linked</h1><p>You can now @mention Moodeng in Slack. You may close this tab.</p></div>
        </body></html>"#
    ).into_response())
}
