use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};
use crate::registry::ActionRegistry;

/// Input for the unified `github` tool.
///
/// Uses a flat struct so the JSON Schema is always `type: "object"`,
/// which all LLM providers (OpenAI, DeepSeek, Anthropic) require.
#[derive(Deserialize, JsonSchema)]
pub struct GithubInput {
    /// The action to perform. One of: list_installations, list_commits,
    /// get_commit, list_directory, read_file, search_code, link_repo, unlink_repo.
    pub action: String,

    /// (list_commits) Branch name. Defaults to the repository's default branch.
    #[serde(default)]
    pub branch: Option<String>,

    /// (list_commits) Maximum number of commits to return (default 10, max 30).
    #[serde(default)]
    pub limit: Option<u8>,

    /// (get_commit) Full 40-character commit SHA.
    #[serde(default)]
    pub sha: Option<String>,

    /// (list_directory, read_file) Path within the repository.
    /// For list_directory: directory path (omit for root).
    /// For read_file: file path (e.g. "src/main.rs").
    #[serde(default)]
    pub path: Option<String>,

    /// (list_directory, read_file) Git ref — branch, tag, or commit SHA.
    /// Defaults to the default branch.
    #[serde(default, rename = "ref")]
    pub git_ref: Option<String>,

    /// (search_code) Search query (e.g. "fn handle_webhook", "class UserService").
    #[serde(default)]
    pub query: Option<String>,

    /// (link_repo) Full GitHub repository URL (e.g. "https://github.com/acme/myrepo").
    #[serde(default)]
    pub repository_url: Option<String>,
}

#[derive(Serialize)]
pub struct GithubOutput {
    pub result: serde_json::Value,
}

pub struct Github;

#[async_trait]
impl PlatformAction for Github {
    type Input = GithubInput;
    type Output = GithubOutput;

    fn name(&self) -> &'static str {
        "github"
    }
    fn description(&self) -> &'static str {
        "Interact with the project's linked GitHub repository. Supports these actions: \
         list_installations (show connected GitHub accounts), \
         list_commits (recent commits, optional branch/limit), \
         get_commit (details for a SHA), \
         list_directory (files in a path, optional ref), \
         read_file (file contents by path, optional ref), \
         search_code (find code by keyword), \
         link_repo (link project to a repository URL), \
         unlink_repo (remove the link)."
    }
    fn required_scope(&self) -> String {
        "observability:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let result = match input.action.as_str() {
            "list_installations" => {
                let resp = ctx.http.watch_get("/api/github/installations").await?;
                resp.json().await?
            }

            "list_commits" => {
                let mut path = format!("/api/projects/{}/github/commits", ctx.project_id);
                let mut params = Vec::new();
                if let Some(l) = input.limit {
                    params.push(format!("limit={}", l));
                }
                if let Some(b) = &input.branch {
                    params.push(format!("branch={}", b));
                }
                if !params.is_empty() {
                    path.push('?');
                    path.push_str(&params.join("&"));
                }
                let resp = ctx.http.watch_get(&path).await?;
                resp.json().await?
            }

            "get_commit" => {
                let sha = input
                    .sha
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("get_commit requires 'sha' parameter"))?;
                let path = format!("/api/projects/{}/github/commit/{}", ctx.project_id, sha);
                let resp = ctx.http.watch_get(&path).await?;
                resp.json().await?
            }

            "list_directory" => {
                let mut url = format!("/api/projects/{}/github/tree", ctx.project_id);
                let mut params = Vec::new();
                if let Some(p) = &input.path {
                    if !p.is_empty() {
                        params.push(format!("path={}", p));
                    }
                }
                if let Some(r) = &input.git_ref {
                    params.push(format!("ref={}", r));
                }
                if !params.is_empty() {
                    url.push('?');
                    url.push_str(&params.join("&"));
                }
                let resp = ctx.http.watch_get(&url).await?;
                resp.json().await?
            }

            "read_file" => {
                let file_path = input
                    .path
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("read_file requires 'path' parameter"))?;
                let mut url = format!(
                    "/api/projects/{}/github/file?path={}",
                    ctx.project_id, file_path
                );
                if let Some(r) = &input.git_ref {
                    url.push_str(&format!("&ref={}", r));
                }
                let resp = ctx.http.watch_get(&url).await?;
                resp.json().await?
            }

            "search_code" => {
                let q = input
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("search_code requires 'query' parameter"))?;
                let url = format!(
                    "/api/projects/{}/github/search?q={}",
                    ctx.project_id,
                    urlencoding::encode(q)
                );
                let resp = ctx.http.watch_get(&url).await?;
                resp.json().await?
            }

            "link_repo" => {
                let repo_url = input.repository_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("link_repo requires 'repository_url' parameter")
                })?;
                let body = serde_json::json!({ "repository_url": repo_url });
                ctx.http
                    .watch_post(&format!("/api/projects/{}/github", ctx.project_id), &body)
                    .await?;
                serde_json::json!({ "success": true })
            }

            "unlink_repo" => {
                ctx.http
                    .watch_delete(&format!("/api/projects/{}/github", ctx.project_id))
                    .await?;
                serde_json::json!({ "success": true })
            }

            other => {
                anyhow::bail!(
                    "Unknown github action '{}'. Valid actions: list_installations, \
                     list_commits, get_commit, list_directory, read_file, search_code, \
                     link_repo, unlink_repo",
                    other
                );
            }
        };

        Ok(GithubOutput { result })
    }
}

pub fn register(registry: &mut ActionRegistry) {
    registry.register(Github);
}
