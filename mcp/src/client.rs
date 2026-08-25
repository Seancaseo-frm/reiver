use reqwest::{Client, Response};
use serde::Serialize;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Pre-configured HTTP client for calling the internal platform APIs
/// (website, flow, watch). Actions use this instead of making raw HTTP calls.
#[derive(Clone)]
pub struct InternalClient {
    http: Client,
    website_url: String,
    flow_url: String,
    watch_url: String,
    herd_url: String,
    project_id: Uuid,
    user_id: Option<Uuid>,
    api_key: String,
    /// Creator type for attribution: "user", "agent", or "system".
    creator_type: Option<String>,
    /// Human-readable label of the agent key (for audit trails).
    creator_key_label: Option<String>,
    /// Last characters of the key for display (masked token suffix).
    creator_key_prefix: Option<String>,
    origin_type: Option<String>,
    origin_ref: Option<String>,
    origin_reason: Option<String>,
}

fn default_client() -> Client {
    Client::builder()
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .build()
        .expect("failed to build HTTP client")
}

impl InternalClient {
    pub fn new(
        website_url: String,
        flow_url: String,
        watch_url: String,
        project_id: Uuid,
        api_key: String,
    ) -> Self {
        Self {
            http: default_client(),
            website_url,
            flow_url,
            watch_url,
            herd_url: String::new(),
            project_id,
            user_id: None,
            api_key,
            creator_type: None,
            creator_key_label: None,
            creator_key_prefix: None,
            origin_type: None,
            origin_ref: None,
            origin_reason: None,
        }
    }

    /// Build a client that authenticates to the website using a user JWT
    /// instead of an agent token. Flow/watch calls still use X-Project-Id.
    pub fn new_for_user(
        website_url: String,
        flow_url: String,
        watch_url: String,
        project_id: Uuid,
        http: Client,
        user_jwt: String,
    ) -> Self {
        Self {
            http,
            website_url,
            flow_url,
            watch_url,
            herd_url: String::new(),
            project_id,
            user_id: None,
            api_key: user_jwt,
            creator_type: None,
            creator_key_label: None,
            creator_key_prefix: None,
            origin_type: None,
            origin_ref: None,
            origin_reason: None,
        }
    }

    /// Set the user ID for service-to-service calls that require user context.
    pub fn with_user_id(mut self, user_id: Uuid) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Set creator attribution for audit trails on write operations.
    pub fn with_creator(mut self, creator_type: &str, key_label: &str, key_prefix: &str) -> Self {
        self.creator_type = Some(creator_type.to_string());
        if !key_label.is_empty() {
            self.creator_key_label = Some(key_label.to_string());
        }
        if !key_prefix.is_empty() {
            self.creator_key_prefix = Some(key_prefix.to_string());
        }
        self
    }

    /// Set origin/causation context for audit trails.
    pub fn with_origin(mut self, origin_type: &str, origin_ref: &str, origin_reason: &str) -> Self {
        self.origin_type = Some(origin_type.to_string());
        self.origin_ref = Some(origin_ref.to_string());
        self.origin_reason = Some(origin_reason.to_string());
        self
    }

    /// Set the Herd service URL for A2A operations.
    pub fn with_herd_url(mut self, url: String) -> Self {
        self.herd_url = url;
        self
    }

    // ── Website API helpers ──────────────────────────────────────────
    // Website calls use X-Project-Id header for service-to-service auth,
    // plus Bearer token when available.

    fn website_request(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = req.header("X-Project-Id", self.project_id.to_string());
        if !self.api_key.is_empty() {
            req = req.bearer_auth(&self.api_key);
        }
        if let Some(uid) = self.user_id {
            req = req.header("X-User-Id", uid.to_string());
        }
        if let Some(ref ct) = self.creator_type {
            req = req.header("X-Creator-Type", ct.as_str());
        }
        if let Some(ref label) = self.creator_key_label {
            req = req.header("X-Creator-Key-Label", label.as_str());
        }
        if let Some(ref kp) = self.creator_key_prefix {
            req = req.header("X-Creator-Key-Prefix", kp.as_str());
        }
        if let Some(ref ot) = self.origin_type {
            req = req.header("X-Audit-Origin-Type", ot.as_str());
        }
        if let Some(ref or_ref) = self.origin_ref {
            req = req.header("X-Audit-Origin-Ref", or_ref.as_str());
        }
        if let Some(ref or_reason) = self.origin_reason {
            req = req.header("X-Audit-Origin-Reason", or_reason.as_str());
        }
        req
    }

    pub async fn website_get(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.website_url, path);
        let resp = self
            .website_request(self.http.get(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn website_post<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.website_url, path);
        let resp = self
            .website_request(self.http.post(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn website_put<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.website_url, path);
        let resp = self
            .website_request(self.http.put(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn website_patch<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.website_url, path);
        let resp = self
            .website_request(self.http.patch(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn website_delete(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.website_url, path);
        let resp = self
            .website_request(self.http.delete(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    /// Apply standard internal service headers (project + optional user/creator context).
    fn internal_headers(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = req.header("X-Project-Id", self.project_id.to_string());
        if let Some(uid) = self.user_id {
            req = req.header("X-User-Id", uid.to_string());
        }
        if let Some(ref ct) = self.creator_type {
            req = req.header("X-Creator-Type", ct.as_str());
        }
        if let Some(ref label) = self.creator_key_label {
            req = req.header("X-Creator-Key-Label", label.as_str());
        }
        if let Some(ref kp) = self.creator_key_prefix {
            req = req.header("X-Creator-Key-Prefix", kp.as_str());
        }
        if let Some(ref ot) = self.origin_type {
            req = req.header("X-Audit-Origin-Type", ot.as_str());
        }
        if let Some(ref or_ref) = self.origin_ref {
            req = req.header("X-Audit-Origin-Ref", or_ref.as_str());
        }
        if let Some(ref or_reason) = self.origin_reason {
            req = req.header("X-Audit-Origin-Reason", or_reason.as_str());
        }
        req
    }

    // ── Flow API helpers (LLM gateway) ──────────────────────────────

    pub async fn flow_get(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.flow_url, path);
        let resp = self
            .internal_headers(self.http.get(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn flow_post<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.flow_url, path);
        let resp = self
            .internal_headers(self.http.post(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn flow_put<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.flow_url, path);
        let resp = self
            .internal_headers(self.http.put(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn flow_delete(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.flow_url, path);
        let resp = self
            .internal_headers(self.http.delete(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    // ── Watch API helpers (APM) ─────────────────────────────────────

    pub async fn watch_get(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.watch_url, path);
        let resp = self
            .internal_headers(self.http.get(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn watch_post<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.watch_url, path);
        let resp = self
            .internal_headers(self.http.post(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn watch_put<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.watch_url, path);
        let resp = self
            .internal_headers(self.http.put(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn watch_patch<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.watch_url, path);
        let resp = self
            .internal_headers(self.http.patch(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn watch_delete(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.watch_url, path);
        let resp = self
            .internal_headers(self.http.delete(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    // ── Herd API helpers ─────────────────────────────────────────────

    pub async fn herd_get(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.herd_url, path);
        let resp = self
            .internal_headers(self.http.get(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn herd_post<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.herd_url, path);
        let resp = self
            .internal_headers(self.http.post(&url))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub async fn herd_delete(&self, path: &str) -> anyhow::Result<Response> {
        let url = format!("{}{}", self.herd_url, path);
        let resp = self
            .internal_headers(self.http.delete(&url))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp)
    }

    pub fn herd_url(&self) -> &str {
        &self.herd_url
    }

    // ── Accessors ───────────────────────────────────────────────────

    pub fn project_id(&self) -> Uuid {
        self.project_id
    }
}
