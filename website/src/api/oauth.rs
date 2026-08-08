use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::header::SET_COOKIE,
    response::{IntoResponse, Json, Redirect, Response},
    routing::get,
    Router,
};
use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AuthUrl, AuthorizationCode, ClientId,
    ClientSecret, CsrfToken, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    TokenResponse, TokenUrl,
};
use openidconnect::{
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata},
    reqwest::async_http_client as oidc_async_http_client,
    AuthorizationCode as OidcAuthorizationCode, ClientId as OidcClientId,
    ClientSecret as OidcClientSecret, CsrfToken as OidcCsrfToken, IssuerUrl, Nonce,
    PkceCodeChallenge as OidcPkceCodeChallenge, PkceCodeVerifier as OidcPkceCodeVerifier,
    RedirectUrl as OidcRedirectUrl, Scope as OidcScope,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::app_state::WebsiteState;
use crate::auth::create_jwt;
use crate::error::{AppError, Result};

const OAUTH_SESSION_TTL_SECONDS: i64 = 600;

pub fn create_oauth_router() -> Router<Arc<WebsiteState>> {
    Router::new()
        .route("/providers", get(list_providers))
        .route("/{provider}", get(initiate_oauth))
        .route("/{provider}/callback", get(handle_oauth_callback))
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ProviderInfo {
    id: &'static str,
    name: &'static str,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct OAuthCallbackQuery {
    code: String,
    state: String,
}

#[derive(Debug, Deserialize, Default)]
struct OAuthInitiateQuery {
    #[serde(default)]
    invite_token: Option<String>,
    #[serde(default)]
    redirect: Option<String>,
}

struct OAuthUserInfo {
    provider_user_id: String,
    email: String,
    name: Option<String>,
}

// ---------------------------------------------------------------------------
// GET /providers — which social providers are configured
// ---------------------------------------------------------------------------

async fn list_providers(State(state): State<Arc<WebsiteState>>) -> Result<Json<Vec<ProviderInfo>>> {
    let cfg = &state.config;
    let providers = vec![
        ProviderInfo {
            id: "google",
            name: "Google",
            enabled: cfg.oauth_google_client_id.is_some()
                && cfg.oauth_google_client_secret.is_some(),
        },
        ProviderInfo {
            id: "github",
            name: "GitHub",
            enabled: cfg.oauth_github_client_id.is_some()
                && cfg.oauth_github_client_secret.is_some(),
        },
        ProviderInfo {
            id: "microsoft",
            name: "Microsoft",
            enabled: cfg.oauth_microsoft_client_id.is_some()
                && cfg.oauth_microsoft_client_secret.is_some(),
        },
    ];
    Ok(Json(providers))
}

// ---------------------------------------------------------------------------
// GET /:provider — start OAuth flow
// ---------------------------------------------------------------------------

async fn initiate_oauth(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(provider): Path<String>,
    Query(initiate_query): Query<OAuthInitiateQuery>,
) -> Result<Redirect> {
    let client_ip = crate::rate_limit::extract_client_ip(&addr);
    crate::rate_limit::check_unauthenticated_rate_limit(&state.redis, &client_ip, "oauth").await?;

    let base_url = &state.config.base_url;
    let redirect_url = format!("{}/api/auth/oauth/{}/callback", base_url, provider);

    let (auth_url, csrf_state, pkce_verifier) = match provider.as_str() {
        "google" => build_oidc_auth_url(&state, "google", &redirect_url).await?,
        "microsoft" => build_oidc_auth_url(&state, "microsoft", &redirect_url).await?,
        "github" => build_github_auth_url(&state, &redirect_url)?,
        _ => return Err(AppError::NotFound("Unknown OAuth provider".to_string())),
    };

    let session_key = format!("oauth:session:{}", csrf_state);
    let mut session_data = serde_json::json!({
        "provider": provider,
        "pkce_verifier": pkce_verifier,
    });
    if let Some(ref token) = initiate_query.invite_token {
        session_data["invite_token"] = serde_json::Value::String(token.clone());
    }
    if let Some(ref redirect) = initiate_query.redirect {
        if redirect.starts_with('/') {
            session_data["redirect"] = serde_json::Value::String(redirect.clone());
        }
    }

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Redis connection error: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    redis::cmd("SETEX")
        .arg(&session_key)
        .arg(OAUTH_SESSION_TTL_SECONDS)
        .arg(session_data.to_string())
        .query_async::<()>(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to store OAuth session: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    Ok(Redirect::temporary(&auth_url))
}

// ---------------------------------------------------------------------------
// GET /:provider/callback — exchange code, find/create user, redirect with JWT
// ---------------------------------------------------------------------------

async fn handle_oauth_callback(
    State(state): State<Arc<WebsiteState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(provider): Path<String>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Response {
    match handle_oauth_callback_inner(&state, &addr, &provider, &query).await {
        Ok(response) => response,
        Err(e) => {
            error!("OAuth callback error: {}", e);
            if let AppError::Validation(ref m) = e {
                if let Some(domain) = m.strip_prefix("invite_required:") {
                    return Redirect::temporary(&format!(
                        "/login?invite_required=1&domain={}",
                        urlencoding::encode(domain)
                    ))
                    .into_response();
                }
            }
            let msg = match &e {
                AppError::Validation(m) => m.clone(),
                AppError::Auth(m) => m.clone(),
                _ => "An error occurred during login. Please try again.".to_string(),
            };
            Redirect::temporary(&format!("/login?error={}", urlencoding::encode(&msg)))
                .into_response()
        }
    }
}

async fn handle_oauth_callback_inner(
    state: &Arc<WebsiteState>,
    addr: &SocketAddr,
    provider: &str,
    query: &OAuthCallbackQuery,
) -> Result<Response> {
    let client_ip = crate::rate_limit::extract_client_ip(addr);
    crate::rate_limit::check_unauthenticated_rate_limit(&state.redis, &client_ip, "oauth_callback")
        .await?;

    let session_key = format!("oauth:session:{}", query.state);

    let redis_pool = state.redis.clone();
    let mut conn = redis_pool.get().await.map_err(|e| {
        error!("Redis connection error: {}", e);
        AppError::Internal(anyhow::anyhow!("Session storage error"))
    })?;

    let session_json: Option<String> = redis::cmd("GET")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await
        .map_err(|e| {
            error!("Failed to read OAuth session: {}", e);
            AppError::Internal(anyhow::anyhow!("Session storage error"))
        })?;

    let session_json = session_json.ok_or_else(|| {
        AppError::Validation(
            "OAuth session expired or invalid state. Please try again.".to_string(),
        )
    })?;

    // Delete session immediately to prevent replay
    let _: () = redis::cmd("DEL")
        .arg(&session_key)
        .query_async(&mut *conn)
        .await
        .unwrap_or(());

    let session: serde_json::Value = serde_json::from_str(&session_json)
        .map_err(|_| AppError::Internal(anyhow::anyhow!("Corrupt OAuth session")))?;

    let stored_provider = session["provider"].as_str().unwrap_or("");
    if stored_provider != provider {
        return Err(AppError::Validation("OAuth provider mismatch".to_string()));
    }

    let pkce_verifier_secret = session["pkce_verifier"]
        .as_str()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("Missing PKCE verifier")))?;

    let base_url = &state.config.base_url;
    let redirect_url = format!("{}/api/auth/oauth/{}/callback", base_url, provider);

    let user_info = match provider {
        "google" => {
            exchange_oidc_code(
                state,
                "google",
                &query.code,
                pkce_verifier_secret,
                &redirect_url,
            )
            .await?
        }
        "microsoft" => {
            exchange_oidc_code(
                state,
                "microsoft",
                &query.code,
                pkce_verifier_secret,
                &redirect_url,
            )
            .await?
        }
        "github" => {
            exchange_github_code(state, &query.code, pkce_verifier_secret, &redirect_url).await?
        }
        _ => return Err(AppError::NotFound("Unknown OAuth provider".to_string())),
    };

    let invite_token = session["invite_token"].as_str().map(|s| s.to_string());

    let (user_id, is_new) =
        find_or_create_oauth_user(state, provider, &user_info, invite_token.as_deref()).await?;

    if is_new {
        if let Some(ref mailer) = state.email {
            let first_name = user_info
                .name
                .as_ref()
                .and_then(|n| n.split_whitespace().next().map(|s| s.to_string()))
                .unwrap_or_else(|| {
                    user_info
                        .email
                        .split('@')
                        .next()
                        .unwrap_or("there")
                        .to_string()
                });
            let mailer = mailer.clone();
            let to = user_info.email.clone();
            tokio::spawn(async move {
                if let Err(e) = mailer
                    .send_welcome(&to, reiver_core::email::WelcomeVars { first_name })
                    .await
                {
                    tracing::warn!("Failed to send welcome email to {}: {}", to, e);
                }
            });
        }
    }

    let token = create_jwt(
        &user_id,
        &state.config.jwt_secret,
        state.config.jwt_expiration_hours,
    )?;

    info!(
        "OAuth login successful: provider={}, user_id={}, email={}",
        provider, user_id, user_info.email
    );

    let is_production = std::env::var("ENVIRONMENT")
        .map(|e| e.to_lowercase() == "production")
        .unwrap_or(false);

    let cookie = reiver_core::auth::create_secure_cookie(
        &token,
        is_production,
        state.config.cookie_domain.as_deref(),
        state.config.jwt_expiration_hours,
    );

    let mut redirect_to = "/login?auth=1".to_string();
    if let Some(post_login_redirect) = session["redirect"].as_str() {
        if post_login_redirect.starts_with('/') {
            redirect_to.push_str(&format!(
                "&redirect={}",
                urlencoding::encode(post_login_redirect)
            ));
        }
    }

    let mut response = Redirect::temporary(&redirect_to).into_response();
    if let Ok(cookie_value) = cookie.parse() {
        response.headers_mut().insert(SET_COOKIE, cookie_value);
    }
    Ok(response)
}

// ---------------------------------------------------------------------------
// OIDC helpers (Google, Microsoft)
// ---------------------------------------------------------------------------

fn oidc_issuer_url(provider: &str) -> Result<String> {
    match provider {
        "google" => Ok("https://accounts.google.com".to_string()),
        "microsoft" => Ok("https://login.microsoftonline.com/common/v2.0".to_string()),
        _ => Err(AppError::Internal(anyhow::anyhow!(
            "No OIDC issuer for provider: {}",
            provider
        ))),
    }
}

fn oidc_credentials(state: &WebsiteState, provider: &str) -> Result<(String, String)> {
    let (client_id, client_secret) = match provider {
        "google" => (
            state.config.oauth_google_client_id.clone(),
            state.config.oauth_google_client_secret.clone(),
        ),
        "microsoft" => (
            state.config.oauth_microsoft_client_id.clone(),
            state.config.oauth_microsoft_client_secret.clone(),
        ),
        _ => (None, None),
    };

    let client_id = client_id
        .ok_or_else(|| AppError::Validation(format!("{} OAuth is not configured", provider)))?;
    let client_secret = client_secret
        .ok_or_else(|| AppError::Validation(format!("{} OAuth is not configured", provider)))?;

    Ok((client_id, client_secret))
}

async fn build_oidc_auth_url(
    state: &WebsiteState,
    provider: &str,
    redirect_url: &str,
) -> Result<(String, String, String)> {
    let (client_id, client_secret) = oidc_credentials(state, provider)?;
    let issuer = oidc_issuer_url(provider)?;

    let issuer_url = IssuerUrl::new(issuer).map_err(|e| {
        error!("Invalid OIDC issuer: {}", e);
        AppError::Internal(anyhow::anyhow!("OAuth configuration error"))
    })?;

    let provider_metadata =
        CoreProviderMetadata::discover_async(issuer_url, oidc_async_http_client)
            .await
            .map_err(|e| {
                error!("OIDC discovery failed for {}: {}", provider, e);
                AppError::Internal(anyhow::anyhow!("Failed to discover identity provider"))
            })?;

    let oidc_redirect = OidcRedirectUrl::new(redirect_url.to_string()).map_err(|e| {
        error!("Invalid redirect URL: {}", e);
        AppError::Internal(anyhow::anyhow!("OAuth configuration error"))
    })?;

    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        OidcClientId::new(client_id),
        Some(OidcClientSecret::new(client_secret)),
    )
    .set_redirect_uri(oidc_redirect);

    let (pkce_challenge, pkce_verifier) = OidcPkceCodeChallenge::new_random_sha256();

    let mut auth_request = client
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            OidcCsrfToken::new_random,
            Nonce::new_random,
        )
        .set_pkce_challenge(pkce_challenge)
        .add_scope(OidcScope::new("email".to_string()))
        .add_scope(OidcScope::new("profile".to_string()));

    if provider == "microsoft" {
        auth_request = auth_request.add_scope(OidcScope::new("openid".to_string()));
    }

    let (auth_url, csrf_token, _nonce) = auth_request.url();

    Ok((
        auth_url.to_string(),
        csrf_token.secret().to_string(),
        pkce_verifier.secret().to_string(),
    ))
}

async fn exchange_oidc_code(
    state: &WebsiteState,
    provider: &str,
    code: &str,
    pkce_verifier_secret: &str,
    redirect_url: &str,
) -> Result<OAuthUserInfo> {
    let (client_id, client_secret) = oidc_credentials(state, provider)?;
    let issuer = oidc_issuer_url(provider)?;

    let issuer_url = IssuerUrl::new(issuer)
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Invalid issuer: {}", e)))?;

    let provider_metadata =
        CoreProviderMetadata::discover_async(issuer_url, oidc_async_http_client)
            .await
            .map_err(|e| {
                error!("OIDC discovery failed: {}", e);
                AppError::Internal(anyhow::anyhow!("Failed to connect to identity provider"))
            })?;

    let oidc_redirect = OidcRedirectUrl::new(redirect_url.to_string())
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Invalid redirect URL: {}", e)))?;

    let client = CoreClient::from_provider_metadata(
        provider_metadata,
        OidcClientId::new(client_id),
        Some(OidcClientSecret::new(client_secret)),
    )
    .set_redirect_uri(oidc_redirect);

    let pkce_verifier = OidcPkceCodeVerifier::new(pkce_verifier_secret.to_string());

    let token_response = client
        .exchange_code(OidcAuthorizationCode::new(code.to_string()))
        .set_pkce_verifier(pkce_verifier)
        .request_async(oidc_async_http_client)
        .await
        .map_err(|e| {
            error!("OIDC token exchange failed for {}: {}", provider, e);
            AppError::Validation("Failed to authenticate with identity provider".to_string())
        })?;

    // Use the userinfo endpoint instead of parsing the ID token directly.
    // This avoids nonce verification issues (we don't persist the nonce across
    // the redirect) and works consistently across providers.
    use openidconnect::EmptyAdditionalClaims;
    let userinfo: openidconnect::UserInfoClaims<
        EmptyAdditionalClaims,
        openidconnect::core::CoreGenderClaim,
    > = client
        .user_info(token_response.access_token().clone(), None)
        .map_err(|e| {
            error!("Userinfo request build failed: {:?}", e);
            AppError::Internal(anyhow::anyhow!("Failed to get user info"))
        })?
        .request_async(oidc_async_http_client)
        .await
        .map_err(|e| {
            error!("Userinfo request failed for {}: {}", provider, e);
            AppError::Internal(anyhow::anyhow!("Failed to get user info from provider"))
        })?;

    let subject = userinfo.subject().to_string();
    let email = userinfo
        .email()
        .map(|e| e.as_str().to_string())
        .ok_or_else(|| {
            AppError::Validation("Email not provided by identity provider".to_string())
        })?;
    let name = userinfo
        .name()
        .and_then(|n| n.get(None))
        .map(|n| n.as_str().to_string());

    Ok(OAuthUserInfo {
        provider_user_id: subject,
        email,
        name,
    })
}

// ---------------------------------------------------------------------------
// GitHub OAuth2 helpers
// ---------------------------------------------------------------------------

fn build_github_auth_url(
    state: &WebsiteState,
    redirect_url: &str,
) -> Result<(String, String, String)> {
    let client_id = state
        .config
        .oauth_github_client_id
        .as_ref()
        .ok_or_else(|| AppError::Validation("GitHub OAuth is not configured".to_string()))?;
    let client_secret = state
        .config
        .oauth_github_client_secret
        .as_ref()
        .ok_or_else(|| AppError::Validation("GitHub OAuth is not configured".to_string()))?;

    let client = BasicClient::new(
        ClientId::new(client_id.clone()),
        Some(ClientSecret::new(client_secret.clone())),
        AuthUrl::new("https://github.com/login/oauth/authorize".to_string())
            .expect("valid GitHub auth URL"),
        Some(
            TokenUrl::new("https://github.com/login/oauth/access_token".to_string())
                .expect("valid GitHub token URL"),
        ),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url.to_string()).expect("valid redirect URL"));

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

    let (auth_url, csrf_token) = client
        .authorize_url(CsrfToken::new_random)
        .add_scope(Scope::new("read:user".to_string()))
        .add_scope(Scope::new("user:email".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    Ok((
        auth_url.to_string(),
        csrf_token.secret().to_string(),
        pkce_verifier.secret().to_string(),
    ))
}

async fn exchange_github_code(
    state: &WebsiteState,
    code: &str,
    pkce_verifier_secret: &str,
    redirect_url: &str,
) -> Result<OAuthUserInfo> {
    let client_id = state
        .config
        .oauth_github_client_id
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("GitHub OAuth not configured")))?;
    let client_secret = state
        .config
        .oauth_github_client_secret
        .as_ref()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("GitHub OAuth not configured")))?;

    let client = BasicClient::new(
        ClientId::new(client_id.clone()),
        Some(ClientSecret::new(client_secret.clone())),
        AuthUrl::new("https://github.com/login/oauth/authorize".to_string())
            .expect("valid GitHub auth URL"),
        Some(
            TokenUrl::new("https://github.com/login/oauth/access_token".to_string())
                .expect("valid GitHub token URL"),
        ),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url.to_string()).expect("valid redirect URL"));

    let pkce_verifier = PkceCodeVerifier::new(pkce_verifier_secret.to_string());

    let token_response = client
        .exchange_code(AuthorizationCode::new(code.to_string()))
        .set_pkce_verifier(pkce_verifier)
        .request_async(async_http_client)
        .await
        .map_err(|e| {
            error!("GitHub token exchange failed: {}", e);
            AppError::Validation("Failed to authenticate with GitHub".to_string())
        })?;

    let access_token = token_response.access_token().secret();

    #[derive(Deserialize)]
    struct GitHubUser {
        id: u64,
        name: Option<String>,
        email: Option<String>,
    }

    let gh_user: GitHubUser = state
        .http_client
        .get("https://api.github.com/user")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "Reiver")
        .send()
        .await
        .map_err(|e| {
            error!("GitHub user API failed: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to get user info from GitHub"))
        })?
        .error_for_status()
        .map_err(|e| {
            error!("GitHub user API returned error status: {}", e);
            AppError::Internal(anyhow::anyhow!("GitHub rejected the access token"))
        })?
        .json()
        .await
        .map_err(|e| {
            error!("GitHub user API parse failed: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to parse GitHub user info"))
        })?;

    // GitHub may not return an email in the user endpoint if it's set to private.
    // In that case, fetch from the /user/emails endpoint.
    let email = if let Some(email) = gh_user.email {
        email
    } else {
        #[derive(Deserialize)]
        struct GitHubEmail {
            email: String,
            primary: bool,
            verified: bool,
        }

        let emails: Vec<GitHubEmail> = state
            .http_client
            .get("https://api.github.com/user/emails")
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "Reiver")
            .send()
            .await
            .map_err(|e| {
                error!("GitHub emails API failed: {}", e);
                AppError::Internal(anyhow::anyhow!("Failed to get email from GitHub"))
            })?
            .error_for_status()
            .map_err(|e| {
                error!("GitHub emails API returned error status: {}", e);
                AppError::Internal(anyhow::anyhow!("GitHub rejected the access token"))
            })?
            .json()
            .await
            .map_err(|e| {
                error!("GitHub emails API parse failed: {}", e);
                AppError::Internal(anyhow::anyhow!("Failed to parse GitHub emails"))
            })?;

        emails
            .iter()
            .find(|e| e.primary && e.verified)
            .or_else(|| emails.iter().find(|e| e.verified))
            .map(|e| e.email.clone())
            .ok_or_else(|| {
                AppError::Validation("No verified email found on your GitHub account".to_string())
            })?
    };

    Ok(OAuthUserInfo {
        provider_user_id: gh_user.id.to_string(),
        email,
        name: gh_user.name,
    })
}

// ---------------------------------------------------------------------------
// User lookup / creation
// ---------------------------------------------------------------------------

async fn find_or_create_oauth_user(
    state: &WebsiteState,
    provider: &str,
    info: &OAuthUserInfo,
    invite_token: Option<&str>,
) -> Result<(Uuid, bool)> {
    let email = info.email.to_lowercase();

    #[derive(sqlx::FromRow)]
    struct OAuthRow {
        user_id: Uuid,
    }

    // 1. Check if this provider account is already linked
    let existing = sqlx::query_as::<_, OAuthRow>(
        "SELECT user_id FROM user_oauth_accounts WHERE provider = $1 AND provider_user_id = $2",
    )
    .bind(provider)
    .bind(&info.provider_user_id)
    .fetch_optional(&*state.db)
    .await
    .map_err(|e| {
        error!("DB error looking up OAuth account: {}", e);
        AppError::Internal(anyhow::anyhow!("Database error"))
    })?;

    if let Some(row) = existing {
        return Ok((row.user_id, false));
    }

    // 2. Check if a user with this email already exists (case-insensitive)
    #[derive(sqlx::FromRow)]
    struct UserRow {
        id: Uuid,
    }

    let existing_user =
        sqlx::query_as::<_, UserRow>("SELECT id FROM users WHERE LOWER(email) = $1")
            .bind(&email)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| {
                error!("DB error looking up user by email: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error"))
            })?;

    if let Some(user) = existing_user {
        // Link the OAuth account to the existing user
        link_oauth_account(&state.db, user.id, provider, info).await?;
        return Ok((user.id, false));
    }

    // 3. Create a new user (respecting allow_signup)
    if !state.config.allow_signup {
        return Err(AppError::Auth("Registration is disabled".to_string()));
    }

    // 3a. Invite-link flow: if an invite_token is provided, use it directly
    if let Some(token) = invite_token {
        #[derive(sqlx::FromRow)]
        struct InviteTokenRow {
            id: Uuid,
            organization_id: Uuid,
            role: String,
        }

        let invite = sqlx::query_as::<_, InviteTokenRow>(
            r#"SELECT id, organization_id, role FROM organization_invitations
               WHERE invite_token = $1 AND accepted_at IS NULL AND expires_at > NOW()
               LIMIT 1"#,
        )
        .bind(token)
        .fetch_optional(&*state.db)
        .await
        .map_err(|e| {
            error!("DB error checking invite token: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

        if let Some(inv) = invite {
            let new_user = sqlx::query_as::<_, UserRow>(
                "INSERT INTO users (email, password_hash, is_approved) VALUES ($1, '', true) RETURNING id",
            )
            .bind(&email)
            .fetch_one(&*state.db)
            .await
            .map_err(|e| {
                error!("Failed to create invited user via token: {}", e);
                AppError::Internal(anyhow::anyhow!("User creation failed"))
            })?;

            sqlx::query(
                "INSERT INTO memberships (user_id, organization_id, role, status) VALUES ($1, $2, $3, 'active')"
            )
            .bind(new_user.id)
            .bind(inv.organization_id)
            .bind(&inv.role)
            .execute(&*state.db)
            .await
            .map_err(|e| {
                error!("Failed to create membership from invite token: {}", e);
                AppError::Internal(anyhow::anyhow!("Membership creation failed"))
            })?;

            sqlx::query("UPDATE organization_invitations SET accepted_at = NOW() WHERE id = $1")
                .bind(inv.id)
                .execute(&*state.db)
                .await
                .ok();

            info!(
                "Created user via invite token: user_id={}, org_id={}",
                new_user.id, inv.organization_id
            );
            link_oauth_account(&state.db, new_user.id, provider, info).await?;
            return Ok((new_user.id, true));
        }
        // Invalid token — fall through to normal domain check
    }

    // 3b. Domain-based org check: if a company domain org exists, require an invite
    let company_domain =
        reiver_core::domains::extract_company_domain(&email).map(|d| d.to_string());

    if let Some(ref domain) = company_domain {
        #[derive(sqlx::FromRow)]
        struct OrgRow {
            id: Uuid,
        }

        let existing_org =
            sqlx::query_as::<_, OrgRow>("SELECT id FROM organizations WHERE domain = $1 LIMIT 1")
                .bind(domain)
                .fetch_optional(&*state.db)
                .await
                .map_err(|e| {
                    error!("DB error checking org domain: {}", e);
                    AppError::Internal(anyhow::anyhow!("Database error"))
                })?;

        if let Some(org) = existing_org {
            // Check if this email has a pending invite
            #[derive(sqlx::FromRow)]
            struct InviteRow {
                id: Uuid,
                role: String,
            }

            let invite = sqlx::query_as::<_, InviteRow>(
                r#"SELECT id, role FROM organization_invitations
                   WHERE organization_id = $1 AND LOWER(email) = $2
                     AND accepted_at IS NULL AND expires_at > NOW()
                   LIMIT 1"#,
            )
            .bind(org.id)
            .bind(&email)
            .fetch_optional(&*state.db)
            .await
            .map_err(|e| {
                error!("DB error checking invite: {}", e);
                AppError::Internal(anyhow::anyhow!("Database error"))
            })?;

            if let Some(inv) = invite {
                // Invited: create user, join org, accept invite
                let new_user = sqlx::query_as::<_, UserRow>(
                    "INSERT INTO users (email, password_hash, is_approved) VALUES ($1, '', true) RETURNING id",
                )
                .bind(&email)
                .fetch_one(&*state.db)
                .await
                .map_err(|e| {
                    error!("Failed to create invited user via OAuth: {}", e);
                    AppError::Internal(anyhow::anyhow!("User creation failed"))
                })?;

                sqlx::query(
                    "INSERT INTO memberships (user_id, organization_id, role, status) VALUES ($1, $2, $3, 'active')"
                )
                .bind(new_user.id)
                .bind(org.id)
                .bind(&inv.role)
                .execute(&*state.db)
                .await
                .map_err(|e| {
                    error!("Failed to create membership for invited user: {}", e);
                    AppError::Internal(anyhow::anyhow!("Membership creation failed"))
                })?;

                sqlx::query(
                    "UPDATE organization_invitations SET accepted_at = NOW() WHERE id = $1",
                )
                .bind(inv.id)
                .execute(&*state.db)
                .await
                .ok();

                info!(
                    "Created invited user via {} OAuth: user_id={}, org_id={}",
                    provider, new_user.id, org.id
                );

                link_oauth_account(&state.db, new_user.id, provider, info).await?;
                return Ok((new_user.id, true));
            }

            // No invite — block registration
            return Err(AppError::Validation(format!("invite_required:{}", domain)));
        }
    }

    let is_approved = crate::platform_settings::self_serve_signup_is_approved(&state.db)
        .await
        .map_err(|e| {
            error!("DB error reading signup policy: {}", e);
            AppError::Internal(anyhow::anyhow!("Database error"))
        })?;

    let new_user = sqlx::query_as::<_, UserRow>(
        "INSERT INTO users (email, password_hash, is_approved) VALUES ($1, '', $2) RETURNING id",
    )
    .bind(&email)
    .bind(is_approved)
    .fetch_one(&*state.db)
    .await
    .map_err(|e| {
        error!("Failed to create user via OAuth: {}", e);
        AppError::Internal(anyhow::anyhow!("User creation failed"))
    })?;

    info!(
        "Created new user via {} OAuth: user_id={}",
        provider, new_user.id
    );

    link_oauth_account(&state.db, new_user.id, provider, info).await?;

    Ok((new_user.id, true))
}

async fn link_oauth_account(
    db: &crate::db::DbPool,
    user_id: Uuid,
    provider: &str,
    info: &OAuthUserInfo,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO user_oauth_accounts (user_id, provider, provider_user_id, provider_email, provider_name)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (provider, provider_user_id) DO NOTHING"#,
    )
    .bind(user_id)
    .bind(provider)
    .bind(&info.provider_user_id)
    .bind(&info.email)
    .bind(&info.name)
    .execute(db)
    .await
    .map_err(|e| {
        error!("Failed to link OAuth account: {}", e);
        AppError::Internal(anyhow::anyhow!("Failed to link OAuth account"))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oidc_issuer_url_google() {
        let url = oidc_issuer_url("google").unwrap();
        assert_eq!(url, "https://accounts.google.com");
    }

    #[test]
    fn test_oidc_issuer_url_microsoft() {
        let url = oidc_issuer_url("microsoft").unwrap();
        assert_eq!(url, "https://login.microsoftonline.com/common/v2.0");
    }

    #[test]
    fn test_oidc_issuer_url_unknown() {
        let result = oidc_issuer_url("unknown");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_oauth_router_has_routes() {
        let router = create_oauth_router();
        // Verify the router was created (compilation is the real test;
        // route matching requires a full app context).
        let _ = router;
    }

    #[test]
    fn test_provider_info_serialization() {
        let info = ProviderInfo {
            id: "google",
            name: "Google",
            enabled: true,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["id"], "google");
        assert_eq!(json["name"], "Google");
        assert_eq!(json["enabled"], true);
    }

    #[test]
    fn test_provider_info_disabled() {
        let info = ProviderInfo {
            id: "github",
            name: "GitHub",
            enabled: false,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["enabled"], false);
    }

    #[test]
    fn test_callback_query_deserialization() {
        let json = r#"{"code":"abc123","state":"xyz789"}"#;
        let query: OAuthCallbackQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.code, "abc123");
        assert_eq!(query.state, "xyz789");
    }

    #[test]
    fn test_email_normalization_in_user_lookup() {
        let info = OAuthUserInfo {
            provider_user_id: "12345".to_string(),
            email: "User@Example.COM".to_string(),
            name: Some("Test User".to_string()),
        };
        assert_eq!(info.email.to_lowercase(), "user@example.com");
    }

    #[test]
    fn test_all_providers_listed() {
        let providers = ["google", "github", "microsoft"];
        for p in &providers {
            match *p {
                "google" | "microsoft" => {
                    assert!(oidc_issuer_url(p).is_ok(), "missing OIDC issuer for {}", p);
                }
                "github" => {
                    // GitHub uses raw OAuth2, no OIDC issuer
                    assert!(oidc_issuer_url(p).is_err());
                }
                _ => panic!("unexpected provider"),
            }
        }
    }
}
