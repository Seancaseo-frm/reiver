use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::action::{ActionContext, PlatformAction};

#[derive(Deserialize, JsonSchema)]
pub struct SearchKnowledgeBaseInput {
    /// Natural language query to search the knowledge base
    pub query: String,
    /// Maximum number of results (default: 10)
    #[serde(default)]
    pub limit: Option<u32>,
    /// Optional category filter to narrow results
    #[serde(default)]
    pub category: Option<String>,
}

#[derive(Serialize)]
pub struct KnowledgeBaseResult {
    pub document_title: String,
    pub category: String,
    pub severity: String,
    pub content: String,
    pub chunk_index: i32,
    pub similarity: f64,
}

#[derive(Serialize)]
pub struct SearchKnowledgeBaseOutput {
    pub results: Vec<KnowledgeBaseResult>,
    pub total: usize,
}

pub struct SearchKnowledgeBase;

#[async_trait]
impl PlatformAction for SearchKnowledgeBase {
    type Input = SearchKnowledgeBaseInput;
    type Output = SearchKnowledgeBaseOutput;

    fn name(&self) -> &'static str {
        "search_knowledge_base"
    }
    fn description(&self) -> &'static str {
        "Search the platform knowledge base using semantic similarity. Provide a natural language \
         query describing what you want to know (e.g. 'why do ClickHouse queries show failures', \
         'how to interpret counter metrics'). Optionally filter by category. Returns the most \
         relevant knowledge base entries ranked by semantic similarity."
    }
    fn required_scope(&self) -> String {
        "project:read".into()
    }

    async fn execute(
        &self,
        ctx: &ActionContext,
        input: Self::Input,
    ) -> anyhow::Result<Self::Output> {
        let db = ctx
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Knowledge base search requires database access"))?;
        let embedder = ctx
            .kb_embedder
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Knowledge base search requires embedding model"))?;

        let query_embedding = embedder.embed(vec![input.query]).await?;
        let query_vec = query_embedding
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Embedding returned no vectors"))?;

        let vec_str = format!(
            "[{}]",
            query_vec
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        let limit = input.limit.unwrap_or(10).min(50) as i64;

        let rows = if let Some(ref category) = input.category {
            sqlx::query_as::<_, (String, String, String, String, i32, f64)>(
                "SELECT d.title, d.category, d.severity, c.content, c.chunk_index, \
                    1 - (c.embedding <=> $1::vector) AS similarity \
                 FROM knowledge_base_chunks c \
                 JOIN knowledge_base_documents d ON c.document_id = d.id \
                 WHERE d.enabled = true AND d.embedding_status = 'ready' AND d.category = $2 \
                 ORDER BY c.embedding <=> $1::vector \
                 LIMIT $3",
            )
            .bind(&vec_str)
            .bind(category)
            .bind(limit)
            .fetch_all(db)
            .await?
        } else {
            sqlx::query_as::<_, (String, String, String, String, i32, f64)>(
                "SELECT d.title, d.category, d.severity, c.content, c.chunk_index, \
                    1 - (c.embedding <=> $1::vector) AS similarity \
                 FROM knowledge_base_chunks c \
                 JOIN knowledge_base_documents d ON c.document_id = d.id \
                 WHERE d.enabled = true AND d.embedding_status = 'ready' \
                 ORDER BY c.embedding <=> $1::vector \
                 LIMIT $2",
            )
            .bind(&vec_str)
            .bind(limit)
            .fetch_all(db)
            .await?
        };

        let total = rows.len();
        let results = rows
            .into_iter()
            .map(
                |(document_title, category, severity, content, chunk_index, similarity)| {
                    KnowledgeBaseResult {
                        document_title,
                        category,
                        severity,
                        content,
                        chunk_index,
                        similarity,
                    }
                },
            )
            .collect();

        Ok(SearchKnowledgeBaseOutput { results, total })
    }
}
