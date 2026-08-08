//! GitHub integration service.
//!
//! Provides integration with GitHub for linking exceptions to commits and PRs.
//! Uses the GitHub App authentication model for secure access to repositories.

use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use octocrab::Octocrab;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tracing::{debug, info};

use crate::config::Config;

/// Verify GitHub webhook signature using HMAC-SHA256.
///
/// GitHub signs webhooks with HMAC-SHA256 using the webhook secret.
/// The signature is sent in the `X-Hub-Signature-256` header as `sha256=<hex>`.
///
/// This function uses constant-time comparison via the `hmac` crate's
/// `verify_slice` method to prevent timing attacks.
///
/// # Arguments
/// * `secret` - The webhook secret configured in GitHub App settings
/// * `payload` - The raw webhook request body
/// * `signature` - The value of the `X-Hub-Signature-256` header
///
/// # Returns
/// `true` if the signature is valid, `false` otherwise
///
/// # Example
/// ```ignore
/// use reiver::github::verify_webhook_signature;
///
/// let secret = "my-webhook-secret";
/// let payload = b"{\"action\": \"deleted\"}";
/// let signature = "sha256=...";  // From X-Hub-Signature-256 header
///
/// if verify_webhook_signature(secret, payload, signature) {
///     // Signature valid, process webhook
/// }
/// ```
pub fn verify_webhook_signature(secret: &str, payload: &[u8], signature: &str) -> bool {
    // Signature format: sha256=<hex>
    let expected_prefix = "sha256=";
    if !signature.starts_with(expected_prefix) {
        return false;
    }

    let signature_hex = &signature[expected_prefix.len()..];
    let signature_bytes = match hex::decode(signature_hex) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(payload);

    // Uses constant-time comparison to prevent timing attacks
    mac.verify_slice(&signature_bytes).is_ok()
}

/// GitHub service for interacting with the GitHub API.
///
/// Uses GitHub App authentication to access repositories on behalf of
/// organizations that have installed the Reiver GitHub App.
#[derive(Clone)]
pub struct GitHubService {
    app_id: u64,
    private_key: String,
}

/// GitHub commit response from API.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubCommitResponse {
    pub sha: String,
    pub html_url: String,
    pub commit: GitHubCommitData,
    pub author: Option<GitHubUser>,
    pub committer: Option<GitHubUser>,
}

/// Git commit data within a GitHub commit.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubCommitData {
    pub message: String,
    pub author: Option<GitHubGitActor>,
    pub committer: Option<GitHubGitActor>,
}

/// Git actor (name/email from git commit).
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubGitActor {
    pub name: String,
    pub email: Option<String>,
}

/// GitHub user.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubUser {
    pub login: String,
}

/// GitHub pull request from commits endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubPullRequestSummary {
    pub number: u64,
    pub title: Option<String>,
    pub state: Option<String>,
    pub html_url: Option<String>,
    pub merged_at: Option<String>,
    pub user: Option<GitHubUser>,
}

/// GitHub repository from installations endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRepository {
    pub name: String,
    pub full_name: Option<String>,
    pub private: Option<bool>,
    pub html_url: Option<String>,
    pub owner: Option<GitHubRepositoryOwner>,
}

/// Repository owner.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRepositoryOwner {
    pub login: String,
    #[serde(rename = "type")]
    pub owner_type: Option<String>,
}

/// Installation repositories response.
#[derive(Debug, Clone, Deserialize)]
pub struct InstallationReposResponse {
    pub total_count: u64,
    pub repositories: Vec<GitHubRepository>,
}

/// GitHub App installation details.
/// Returned by GET /app/installations/{installation_id}
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallation {
    pub id: u64,
    pub account: GitHubInstallationAccount,
    /// Permissions granted to the installation
    pub permissions: Option<serde_json::Value>,
    /// Events the installation is subscribed to
    pub events: Option<Vec<String>>,
}

/// Account (user or organization) that installed the GitHub App.
#[derive(Debug, Clone, Deserialize)]
pub struct GitHubInstallationAccount {
    pub login: String,
    pub id: u64,
    #[serde(rename = "type")]
    pub account_type: Option<String>,
    pub html_url: Option<String>,
}

/// Commit information with linked pull requests.
///
/// # Privacy Note
/// The `author_email` field is intentionally included. This data is only accessible
/// through authenticated API endpoints that verify organization membership, ensuring
/// only project members can view commit author details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitWithPulls {
    pub sha: String,
    pub message: String,
    pub author_name: Option<String>,
    /// Git commit author email. Exposed only to authenticated project members.
    pub author_email: Option<String>,
    pub author_login: Option<String>,
    pub committer_name: Option<String>,
    pub committer_login: Option<String>,
    pub html_url: String,
    pub pull_requests: Vec<LinkedPullRequest>,
}

/// Linked pull request information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkedPullRequest {
    pub number: u64,
    pub title: String,
    pub state: String,
    pub html_url: String,
    pub merged: bool,
    pub author_login: Option<String>,
}

impl GitHubService {
    /// Create a new GitHub service from configuration.
    ///
    /// Returns None if GitHub App is not configured.
    pub fn from_config(config: &Config) -> Option<Self> {
        let app_id = config.github_app_id?;
        let private_key = config.github_app_private_key.clone()?;

        Some(Self {
            app_id,
            private_key,
        })
    }

    /// Create a new GitHub service with explicit parameters.
    pub fn new(app_id: u64, private_key: String) -> Self {
        Self {
            app_id,
            private_key,
        }
    }

    /// Check if the service is properly configured.
    pub fn is_configured(&self) -> bool {
        !self.private_key.is_empty()
    }

    /// Create an authenticated Octocrab client for a specific installation.
    ///
    /// The returned client uses an installation access token that is automatically
    /// refreshed by octocrab when it expires (tokens are valid for 1 hour).
    #[tracing::instrument(skip(self), fields(app_id = self.app_id))]
    pub async fn get_installation_client(&self, installation_id: u64) -> Result<Octocrab> {
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(self.private_key.as_bytes())
            .map_err(|e| anyhow!("Invalid GitHub App private key: {}", e))?;

        // Build client with GitHub App credentials and installation ID
        // Octocrab handles installation token generation automatically
        let client = Octocrab::builder()
            .app(self.app_id.into(), key)
            .build()
            .map_err(|e| anyhow!("Failed to build GitHub App client: {}", e))?;

        // Use the installation method to get a client scoped to the installation
        // This returns a Result, not a Future
        let installation_client = client
            .installation(installation_id.into())
            .map_err(|e| anyhow!("Failed to get installation client: {}", e))?;

        debug!(
            app_id = self.app_id,
            installation_id = installation_id,
            "Created GitHub installation client"
        );

        Ok(installation_client)
    }

    /// Fetch commit details from a repository.
    ///
    /// # Arguments
    /// * `installation_id` - The GitHub App installation ID
    /// * `owner` - Repository owner (org or user)
    /// * `repo` - Repository name
    /// * `sha` - Commit SHA
    #[tracing::instrument(skip(self))]
    pub async fn get_commit(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<GitHubCommitResponse> {
        let client = self.get_installation_client(installation_id).await?;
        Self::fetch_commit(&client, owner, repo, sha).await
    }

    /// Internal: Fetch commit using an existing client.
    async fn fetch_commit(
        client: &Octocrab,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<GitHubCommitResponse> {
        // GitHub API: GET /repos/{owner}/{repo}/commits/{ref}
        let route = format!("/repos/{}/{}/commits/{}", owner, repo, sha);
        let commit: GitHubCommitResponse = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Failed to fetch commit {}: {}", sha, e))?;

        debug!(
            owner = owner,
            repo = repo,
            sha = sha,
            "Fetched commit from GitHub"
        );

        Ok(commit)
    }

    /// Get pull requests associated with a commit.
    ///
    /// Uses the GitHub API to find PRs that contain this commit.
    ///
    /// # Arguments
    /// * `installation_id` - The GitHub App installation ID
    /// * `owner` - Repository owner (org or user)
    /// * `repo` - Repository name
    /// * `sha` - Commit SHA
    #[tracing::instrument(skip(self))]
    pub async fn get_commit_pulls(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<GitHubPullRequestSummary>> {
        let client = self.get_installation_client(installation_id).await?;
        Self::fetch_commit_pulls(&client, owner, repo, sha).await
    }

    /// Internal: Fetch commit PRs using an existing client.
    async fn fetch_commit_pulls(
        client: &Octocrab,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<Vec<GitHubPullRequestSummary>> {
        // GitHub API: GET /repos/{owner}/{repo}/commits/{commit_sha}/pulls
        let route = format!("/repos/{}/{}/commits/{}/pulls", owner, repo, sha);
        let pulls: Vec<GitHubPullRequestSummary> = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Failed to fetch PRs for commit {}: {}", sha, e))?;

        debug!(
            owner = owner,
            repo = repo,
            sha = sha,
            pull_count = pulls.len(),
            "Fetched pull requests for commit"
        );

        Ok(pulls)
    }

    /// Get commit details with linked pull requests.
    ///
    /// This is a convenience method that fetches both the commit and its
    /// associated PRs in one call. Creates a single client and reuses it
    /// for both API calls to avoid redundant token exchanges.
    #[tracing::instrument(skip(self))]
    pub async fn get_commit_with_pulls(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<CommitWithPulls> {
        // Create client once and reuse for both API calls
        let client = self.get_installation_client(installation_id).await?;

        // Fetch commit and PRs in parallel using the same client
        let (commit_result, pulls_result) = tokio::join!(
            Self::fetch_commit(&client, owner, repo, sha),
            Self::fetch_commit_pulls(&client, owner, repo, sha)
        );

        let commit = commit_result?;
        let pulls = pulls_result.unwrap_or_else(|e| {
            // Log but don't fail if we can't get PRs
            debug!(error = %e, "Failed to fetch PRs for commit, continuing without");
            vec![]
        });

        let linked_prs: Vec<LinkedPullRequest> = pulls
            .into_iter()
            .map(|pr| LinkedPullRequest {
                number: pr.number,
                title: pr.title.unwrap_or_default(),
                state: pr.state.unwrap_or_default(),
                html_url: pr.html_url.unwrap_or_default(),
                merged: pr.merged_at.is_some(),
                author_login: pr.user.map(|u| u.login),
            })
            .collect();

        Ok(CommitWithPulls {
            sha: commit.sha.clone(),
            message: commit.commit.message.clone(),
            author_name: commit.commit.author.as_ref().map(|a| a.name.clone()),
            author_email: commit.commit.author.as_ref().and_then(|a| a.email.clone()),
            author_login: commit.author.as_ref().map(|a| a.login.clone()),
            committer_name: commit.commit.committer.as_ref().map(|c| c.name.clone()),
            committer_login: commit.committer.as_ref().map(|c| c.login.clone()),
            html_url: commit.html_url.clone(),
            pull_requests: linked_prs,
        })
    }

    /// List recent commits on a branch (default: repo default branch).
    ///
    /// Returns up to `per_page` commits ordered newest-first.
    #[tracing::instrument(skip(self))]
    pub async fn list_recent_commits(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        branch: Option<&str>,
        per_page: u8,
    ) -> Result<Vec<CommitWithPulls>> {
        let client = self.get_installation_client(installation_id).await?;

        let mut route = format!("/repos/{}/{}/commits?per_page={}", owner, repo, per_page);
        if let Some(b) = branch {
            route.push_str(&format!("&sha={}", b));
        }

        let raw_commits: Vec<GitHubCommitResponse> = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Failed to list commits for {}/{}: {}", owner, repo, e))?;

        let commits = raw_commits
            .into_iter()
            .map(|c| CommitWithPulls {
                sha: c.sha,
                message: c.commit.message,
                author_name: c.commit.author.as_ref().map(|a| a.name.clone()),
                author_email: c.commit.author.as_ref().and_then(|a| a.email.clone()),
                author_login: c.author.as_ref().map(|a| a.login.clone()),
                committer_name: c.commit.committer.as_ref().map(|c| c.name.clone()),
                committer_login: c.committer.as_ref().map(|c| c.login.clone()),
                html_url: c.html_url,
                pull_requests: Vec::new(),
            })
            .collect();

        Ok(commits)
    }

    /// Verify and get installation details from GitHub.
    ///
    /// This method verifies that the installation exists and is accessible by our
    /// GitHub App, and returns the installation metadata including account info.
    ///
    /// # Security
    /// Use this to verify an installation_id from a callback before storing it.
    /// This prevents attackers from associating arbitrary installation IDs.
    ///
    /// # Arguments
    /// * `installation_id` - The GitHub App installation ID to verify
    ///
    /// # Returns
    /// Installation details if the installation exists and is accessible.
    /// Returns an error if the installation doesn't exist or is not accessible.
    #[tracing::instrument(skip(self), fields(app_id = self.app_id))]
    pub async fn get_installation(&self, installation_id: u64) -> Result<GitHubInstallation> {
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(self.private_key.as_bytes())
            .map_err(|e| anyhow!("Invalid GitHub App private key: {}", e))?;

        // Build app-level client (not installation-scoped) to query our own installations
        let client = Octocrab::builder()
            .app(self.app_id.into(), key)
            .build()
            .map_err(|e| anyhow!("Failed to build GitHub App client: {}", e))?;

        // GitHub API: GET /app/installations/{installation_id}
        let route = format!("/app/installations/{}", installation_id);
        let installation: GitHubInstallation =
            client.get(&route, None::<&()>).await.map_err(|e| {
                anyhow!(
                    "Installation {} not found or not accessible: {}",
                    installation_id,
                    e
                )
            })?;

        info!(
            installation_id = installation_id,
            account_login = %installation.account.login,
            account_type = ?installation.account.account_type,
            "Verified GitHub installation"
        );

        Ok(installation)
    }

    /// List repositories accessible to an installation.
    ///
    /// Used to populate the list of repositories when linking a project.
    /// Automatically paginates through all results (GitHub returns max 100 per page).
    ///
    /// # Limits
    /// Stops after 10 pages (1000 repos) to prevent excessive API calls.
    /// Installations with more repos should use incremental sync via webhooks.
    #[tracing::instrument(skip(self))]
    pub async fn list_installation_repos(
        &self,
        installation_id: u64,
    ) -> Result<Vec<GitHubRepository>> {
        let client = self.get_installation_client(installation_id).await?;

        const PER_PAGE: u32 = 100;
        const MAX_PAGES: u32 = 10; // Safety limit: max 1000 repos

        let mut all_repos = Vec::new();
        let mut page: u32 = 1;

        loop {
            // GitHub API: GET /installation/repositories?per_page=100&page=N
            let route = format!(
                "/installation/repositories?per_page={}&page={}",
                PER_PAGE, page
            );
            let response: InstallationReposResponse = client
                .get(&route, None::<&()>)
                .await
                .map_err(|e| anyhow!("Failed to list installation repos (page {}): {}", page, e))?;

            let repos_count = response.repositories.len();
            all_repos.extend(response.repositories);

            debug!(
                installation_id = installation_id,
                page = page,
                repos_in_page = repos_count,
                total_count = response.total_count,
                "Fetched installation repositories page"
            );

            // Check if we've fetched all repos or hit the safety limit
            if all_repos.len() >= response.total_count as usize || repos_count < PER_PAGE as usize {
                break;
            }

            page += 1;
            if page > MAX_PAGES {
                info!(
                    installation_id = installation_id,
                    total_fetched = all_repos.len(),
                    total_available = response.total_count,
                    "Stopped pagination at max pages limit"
                );
                break;
            }
        }

        info!(
            installation_id = installation_id,
            repo_count = all_repos.len(),
            "Listed installation repositories"
        );

        Ok(all_repos)
    }

    /// Fetch file contents from a repository at a given git ref.
    ///
    /// Uses the GitHub Contents API to retrieve a single file. The API returns
    /// base64-encoded content which this method decodes to a UTF-8 string.
    ///
    /// # Arguments
    /// * `installation_id` - The GitHub App installation ID
    /// * `owner` - Repository owner (org or user)
    /// * `repo` - Repository name
    /// * `path` - File path within the repo (e.g., `"src/main.rs"`)
    /// * `git_ref` - Optional git ref (branch, tag, or commit SHA). Uses default branch if None.
    ///
    /// # Security
    /// The `path` is validated to prevent directory traversal attacks.
    #[tracing::instrument(skip(self))]
    pub async fn get_file_contents(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        path: &str,
        git_ref: Option<&str>,
    ) -> Result<FileContents> {
        // Validate path to prevent directory traversal
        if !is_valid_file_path(path) {
            return Err(anyhow!("Invalid file path: {}", path));
        }

        let client = self.get_installation_client(installation_id).await?;

        // GitHub API: GET /repos/{owner}/{repo}/contents/{path}?ref={ref}
        // URL-encode path segments and the ref query parameter to handle
        // special characters like spaces, #, &, etc.
        let encoded_path = path
            .split('/')
            .map(|seg| urlencoding::encode(seg))
            .collect::<Vec<_>>()
            .join("/");
        let route = if let Some(r) = git_ref {
            format!(
                "/repos/{}/{}/contents/{}?ref={}",
                owner,
                repo,
                encoded_path,
                urlencoding::encode(r)
            )
        } else {
            format!("/repos/{}/{}/contents/{}", owner, repo, encoded_path)
        };

        let response: GitHubFileResponse = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Failed to fetch file {}: {}", path, e))?;

        // Decode base64 content.
        // The GitHub Contents API omits the `content` field for files > 1MB.
        let content = if let Some(encoded) = &response.content {
            // GitHub sends base64 with newlines; strip them before decoding
            let cleaned = encoded.replace('\n', "");
            let bytes = base64_decode(&cleaned)
                .map_err(|e| anyhow!("Failed to decode base64 content for {}: {}", path, e))?;
            String::from_utf8(bytes)
                .map_err(|e| anyhow!("File {} is not valid UTF-8: {}", path, e))?
        } else if response.size > 0 {
            return Err(anyhow!(
                "File {} is too large ({} bytes) for the GitHub Contents API (max 1MB)",
                path,
                response.size
            ));
        } else {
            String::new()
        };

        debug!(
            owner = owner,
            repo = repo,
            path = path,
            size = response.size,
            "Fetched file contents from GitHub"
        );

        Ok(FileContents {
            path: response.path,
            content,
            sha: response.sha,
            size: response.size,
            html_url: response.html_url,
        })
    }

    /// List directory contents in a repository.
    ///
    /// Uses the GitHub Contents API which returns an array of entries when
    /// the path points to a directory.
    #[tracing::instrument(skip(self))]
    pub async fn list_directory(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        path: Option<&str>,
        git_ref: Option<&str>,
    ) -> Result<Vec<DirectoryEntry>> {
        let client = self.get_installation_client(installation_id).await?;

        let route = match (path, git_ref) {
            (Some(p), Some(r)) => {
                if !is_valid_file_path(p) {
                    return Err(anyhow!("Invalid path: {}", p));
                }
                let encoded = p
                    .split('/')
                    .map(|s| urlencoding::encode(s))
                    .collect::<Vec<_>>()
                    .join("/");
                format!(
                    "/repos/{}/{}/contents/{}?ref={}",
                    owner,
                    repo,
                    encoded,
                    urlencoding::encode(r)
                )
            }
            (Some(p), None) => {
                if !is_valid_file_path(p) {
                    return Err(anyhow!("Invalid path: {}", p));
                }
                let encoded = p
                    .split('/')
                    .map(|s| urlencoding::encode(s))
                    .collect::<Vec<_>>()
                    .join("/");
                format!("/repos/{}/{}/contents/{}", owner, repo, encoded)
            }
            (None, Some(r)) => format!(
                "/repos/{}/{}/contents?ref={}",
                owner,
                repo,
                urlencoding::encode(r)
            ),
            (None, None) => format!("/repos/{}/{}/contents", owner, repo),
        };

        let entries: Vec<DirectoryEntry> = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Failed to list directory: {}", e))?;

        Ok(entries)
    }

    /// Search code in a repository.
    ///
    /// Uses the GitHub Code Search API to find files matching a query.
    #[tracing::instrument(skip(self))]
    pub async fn search_code(
        &self,
        installation_id: u64,
        owner: &str,
        repo: &str,
        query: &str,
    ) -> Result<Vec<CodeSearchResult>> {
        let client = self.get_installation_client(installation_id).await?;

        let full_query = format!("{} repo:{}/{}", query, owner, repo);
        let route = format!(
            "/search/code?q={}&per_page=20",
            urlencoding::encode(&full_query)
        );

        let response: CodeSearchResponse = client
            .get(&route, None::<&()>)
            .await
            .map_err(|e| anyhow!("Code search failed: {}", e))?;

        Ok(response.items)
    }
}

/// Entry in a directory listing from the GitHub Contents API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    /// "file", "dir", or "symlink"
    #[serde(rename = "type")]
    pub entry_type: String,
    pub size: u64,
    pub html_url: Option<String>,
}

/// GitHub Code Search API response.
#[derive(Debug, Clone, Deserialize)]
struct CodeSearchResponse {
    items: Vec<CodeSearchResult>,
}

/// A single code search hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeSearchResult {
    pub name: String,
    pub path: String,
    pub html_url: String,
    pub repository: Option<CodeSearchRepo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeSearchRepo {
    pub full_name: String,
}

/// Decoded file contents from a GitHub repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileContents {
    /// File path within the repository
    pub path: String,
    /// Decoded file content as UTF-8 text
    pub content: String,
    /// Git blob SHA for the file
    pub sha: String,
    /// File size in bytes
    pub size: u64,
    /// URL to view the file on GitHub
    pub html_url: Option<String>,
}

/// Raw response from the GitHub Contents API.
#[derive(Debug, Clone, Deserialize)]
struct GitHubFileResponse {
    path: String,
    sha: String,
    size: u64,
    content: Option<String>,
    html_url: Option<String>,
}

/// Simple base64 decoder (standard alphabet).
fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, anyhow::Error> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .map_err(|e| anyhow!("base64 decode error: {}", e))
}

/// Validate a file path to prevent directory traversal attacks.
///
/// Rejects paths containing `..`, starting with `/`, or containing null bytes.
fn is_valid_file_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains("..")
        && !path.starts_with('/')
        && !path.contains('\0')
        && path.len() <= 1024
}

/// Validate that a GitHub owner or repository name contains only allowed characters.
///
/// GitHub allows alphanumeric characters, hyphens, underscores, and periods in
/// owner/org names and repository names. This validation prevents path injection
/// attacks when constructing API routes.
///
/// # Security
/// This is a defense-in-depth measure. While GitHub's API would reject malformed
/// paths, validating early prevents potential issues and provides clearer errors.
fn is_valid_github_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        // GitHub doesn't allow names starting with a dot or consisting only of dots
        && !name.starts_with('.')
        && name.chars().any(|c| c != '.')
}

/// Strip query string and fragment from a URL path segment.
///
/// Handles cases like:
/// - "repo?foo=bar" -> "repo"
/// - "repo#section" -> "repo"
/// - "repo?foo=bar#section" -> "repo"
fn strip_query_and_fragment(segment: &str) -> &str {
    segment
        .split('?')
        .next()
        .unwrap_or(segment)
        .split('#')
        .next()
        .unwrap_or(segment)
}

/// Parse a GitHub repository URL into owner and repo components.
///
/// Supports formats:
/// - https://github.com/owner/repo
/// - https://github.com/owner/repo.git
/// - git@github.com:owner/repo.git
///
/// # Security
/// Validates that owner and repo names contain only allowed characters
/// (alphanumeric, `-`, `_`, `.`) to prevent path injection attacks.
///
/// # Returns
/// Tuple of (owner, repo) if successful and valid, None otherwise
pub fn parse_repo_url(url: &str) -> Option<(String, String)> {
    // HTTPS format: https://github.com/owner/repo or https://github.com/owner/repo.git
    if url.contains("github.com/") {
        let parts: Vec<&str> = url.split("github.com/").collect();
        if parts.len() == 2 {
            let path = parts[1].trim_end_matches(".git").trim_end_matches('/');
            let segments: Vec<&str> = path.split('/').collect();
            if segments.len() >= 2 {
                let owner = segments[0];
                // Strip query strings and fragments from repo name (last segment we care about)
                let repo = strip_query_and_fragment(segments[1]);

                // Validate both owner and repo names
                if is_valid_github_name(owner) && is_valid_github_name(repo) {
                    return Some((owner.to_string(), repo.to_string()));
                }
            }
        }
    }

    // SSH format: git@github.com:owner/repo.git
    if url.starts_with("git@github.com:") {
        let path = url
            .trim_start_matches("git@github.com:")
            .trim_end_matches(".git");
        let segments: Vec<&str> = path.split('/').collect();
        if segments.len() >= 2 {
            let owner = segments[0];
            // Strip query strings and fragments from repo name
            let repo = strip_query_and_fragment(segments[1]);

            // Validate both owner and repo names
            if is_valid_github_name(owner) && is_valid_github_name(repo) {
                return Some((owner.to_string(), repo.to_string()));
            }
        }
    }

    None
}

/// Check if an exception fingerprint was first seen in a specific version.
///
/// This is used to display "Introduced in this version" badges on exceptions.
///
/// # Arguments
/// * `clickhouse` - ClickHouse client
/// * `project_id` - Project ID
/// * `fingerprint` - Exception fingerprint
/// * `version` - Version to check (git commit SHA or release tag)
///
/// # Returns
/// `true` if this is the first version where the fingerprint was seen
pub async fn is_first_seen_in_version(
    clickhouse: &clickhouse::Client,
    project_id: &str,
    fingerprint: &str,
    version: &str,
) -> Result<bool> {
    if version.is_empty() {
        return Ok(false);
    }

    // Query to find the earliest version where this fingerprint was seen
    // We order by timestamp to get the first occurrence
    let query = r#"
        SELECT
            service_version
        FROM reiver.exceptions
        WHERE project_id = ?
            AND fingerprint = ?
            AND service_version != ''
        ORDER BY timestamp ASC
        LIMIT 1
    "#;

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct FirstVersionRow {
        service_version: String,
    }

    let result: Option<FirstVersionRow> = clickhouse
        .query(query)
        .bind(project_id)
        .bind(fingerprint)
        .fetch_optional()
        .await
        .map_err(|e| anyhow!("Failed to query first seen version: {}", e))?;

    match result {
        Some(row) => Ok(row.service_version == version),
        None => Ok(false), // No version data available
    }
}

/// Get version introduction information for an exception.
///
/// Returns the version where an exception fingerprint was first seen,
/// along with context about whether it's new in the specified version.
#[derive(Debug, Clone, serde::Serialize)]
pub struct VersionIntroductionInfo {
    /// The version where this exception was first seen
    pub first_seen_version: Option<String>,
    /// When the exception was first seen (timestamp)
    pub first_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether the exception is new in the current/specified version
    pub is_new_in_version: bool,
    /// Total number of unique versions that have seen this exception
    pub version_count: u64,
}

/// Get version introduction information for an exception fingerprint.
///
/// # Arguments
/// * `clickhouse` - ClickHouse client
/// * `project_id` - Project ID
/// * `fingerprint` - Exception fingerprint
/// * `current_version` - Current version to check against (optional)
pub async fn get_version_introduction_info(
    clickhouse: &clickhouse::Client,
    project_id: &str,
    fingerprint: &str,
    current_version: Option<&str>,
) -> Result<VersionIntroductionInfo> {
    // Query to get version introduction details
    let query = r#"
        SELECT
            argMin(service_version, timestamp) as first_version,
            min(timestamp) as first_seen_at,
            uniqExact(service_version) as version_count
        FROM reiver.exceptions
        WHERE project_id = ?
            AND fingerprint = ?
            AND service_version != ''
    "#;

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct VersionInfoRow {
        first_version: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_seen_at: chrono::DateTime<chrono::Utc>,
        version_count: u64,
    }

    let result: Option<VersionInfoRow> = clickhouse
        .query(query)
        .bind(project_id)
        .bind(fingerprint)
        .fetch_optional()
        .await
        .map_err(|e| anyhow!("Failed to query version introduction info: {}", e))?;

    match result {
        Some(row) if !row.first_version.is_empty() => {
            let is_new = current_version
                .map(|cv| cv == row.first_version)
                .unwrap_or(false);

            Ok(VersionIntroductionInfo {
                first_seen_version: Some(row.first_version),
                first_seen_at: Some(row.first_seen_at),
                is_new_in_version: is_new,
                version_count: row.version_count,
            })
        }
        _ => Ok(VersionIntroductionInfo {
            first_seen_version: None,
            first_seen_at: None,
            is_new_in_version: false,
            version_count: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // is_valid_github_name tests
    // ==========================================================================

    #[test]
    fn test_valid_github_names() {
        assert!(is_valid_github_name("acme"));
        assert!(is_valid_github_name("acme-corp"));
        assert!(is_valid_github_name("acme_corp"));
        assert!(is_valid_github_name("acme.corp"));
        assert!(is_valid_github_name("Acme123"));
        assert!(is_valid_github_name("my-repo.js"));
    }

    #[test]
    fn test_invalid_github_names() {
        // Empty
        assert!(!is_valid_github_name(""));
        // Path traversal attempts
        assert!(!is_valid_github_name(".."));
        assert!(!is_valid_github_name("../other"));
        assert!(!is_valid_github_name("owner/.."));
        // Starts with dot (not allowed by GitHub)
        assert!(!is_valid_github_name(".hidden"));
        // Special characters
        assert!(!is_valid_github_name("owner@evil"));
        assert!(!is_valid_github_name("owner:evil"));
        assert!(!is_valid_github_name("owner/evil"));
        assert!(!is_valid_github_name("owner?evil"));
        assert!(!is_valid_github_name("owner#evil"));
    }

    // ==========================================================================
    // strip_query_and_fragment tests
    // ==========================================================================

    #[test]
    fn test_strip_query_string() {
        assert_eq!(strip_query_and_fragment("repo?foo=bar"), "repo");
        assert_eq!(strip_query_and_fragment("repo?foo=bar&baz=qux"), "repo");
    }

    #[test]
    fn test_strip_fragment() {
        assert_eq!(strip_query_and_fragment("repo#section"), "repo");
    }

    #[test]
    fn test_strip_query_and_fragment_combined() {
        assert_eq!(strip_query_and_fragment("repo?foo=bar#section"), "repo");
    }

    #[test]
    fn test_strip_nothing() {
        assert_eq!(strip_query_and_fragment("repo"), "repo");
    }

    // ==========================================================================
    // parse_repo_url tests
    // ==========================================================================

    #[test]
    fn test_parse_repo_url_https() {
        let result = parse_repo_url("https://github.com/acme/myrepo");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_https_with_git() {
        let result = parse_repo_url("https://github.com/acme/myrepo.git");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_ssh() {
        let result = parse_repo_url("git@github.com:acme/myrepo.git");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_trailing_slash() {
        let result = parse_repo_url("https://github.com/acme/myrepo/");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_invalid() {
        let result = parse_repo_url("https://gitlab.com/acme/myrepo");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_with_query_string() {
        let result = parse_repo_url("https://github.com/acme/myrepo?tab=readme");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_with_fragment() {
        let result = parse_repo_url("https://github.com/acme/myrepo#installation");
        assert_eq!(result, Some(("acme".to_string(), "myrepo".to_string())));
    }

    #[test]
    fn test_parse_repo_url_empty_owner() {
        // github.com//repo - empty owner segment
        let result = parse_repo_url("https://github.com//myrepo");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_empty_repo() {
        // github.com/owner/ - no repo segment after owner
        let result = parse_repo_url("https://github.com/acme/");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_path_traversal_owner() {
        // Attempt to use path traversal in owner
        let result = parse_repo_url("https://github.com/../other-owner/repo");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_path_traversal_repo() {
        // Attempt to use path traversal in repo
        let result = parse_repo_url("https://github.com/owner/../other-repo");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_special_chars_rejected() {
        // Special characters should be rejected
        let result = parse_repo_url("https://github.com/owner@evil/repo");
        assert_eq!(result, None);

        let result = parse_repo_url("https://github.com/owner/repo:tag");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_repo_url_valid_special_chars() {
        // Valid GitHub characters: hyphen, underscore, dot
        let result = parse_repo_url("https://github.com/my-org/my_repo.js");
        assert_eq!(
            result,
            Some(("my-org".to_string(), "my_repo.js".to_string()))
        );
    }
}
