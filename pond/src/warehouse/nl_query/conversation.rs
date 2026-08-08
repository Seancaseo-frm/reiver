//! Conversation persistence for multi-turn NL queries.
//!
//! Each conversation belongs to a project+user and contains an ordered
//! sequence of turns (question -> SQL -> result metadata).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// A single turn in a conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationTurn {
    pub turn_index: i32,
    pub question: String,
    pub generated_sql: String,
    pub execution_time_ms: Option<i32>,
    pub row_count: Option<i32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// A successful question/SQL pair used for few-shot prompting.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub question: String,
    pub sql: String,
}

/// Repository for NL conversation persistence.
pub struct ConversationRepository {
    db: PgPool,
}

const MAX_TURNS_LOADED: i64 = 10;

impl ConversationRepository {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Create a new conversation and return its ID.
    pub async fn create(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> Result<Uuid, sqlx::Error> {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO nl_conversations (id, project_id, user_id) VALUES ($1, $2, $3)",
        )
        .bind(id)
        .bind(project_id)
        .bind(user_id)
        .execute(&self.db)
        .await?;
        Ok(id)
    }

    /// Load the most recent turns for a conversation (up to MAX_TURNS_LOADED).
    pub async fn load_turns(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<ConversationTurn>, sqlx::Error> {
        let rows = sqlx::query(
            r#"SELECT turn_index, question, generated_sql, execution_time_ms,
                      row_count, error, created_at
               FROM nl_conversation_turns
               WHERE conversation_id = $1
               ORDER BY turn_index ASC
               LIMIT $2"#,
        )
        .bind(conversation_id)
        .bind(MAX_TURNS_LOADED)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| ConversationTurn {
                turn_index: r.get("turn_index"),
                question: r.get("question"),
                generated_sql: r.get("generated_sql"),
                execution_time_ms: r.get("execution_time_ms"),
                row_count: r.get("row_count"),
                error: r.get("error"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Get the next turn index for a conversation.
    pub async fn next_turn_index(
        &self,
        conversation_id: Uuid,
    ) -> Result<i32, sqlx::Error> {
        let max: Option<i32> = sqlx::query_scalar(
            "SELECT MAX(turn_index) FROM nl_conversation_turns WHERE conversation_id = $1",
        )
        .bind(conversation_id)
        .fetch_one(&self.db)
        .await?;
        Ok(max.map(|m| m + 1).unwrap_or(0))
    }

    /// Insert a new turn into a conversation.
    pub async fn insert_turn(
        &self,
        conversation_id: Uuid,
        turn_index: i32,
        question: &str,
        generated_sql: &str,
        execution_time_ms: Option<i32>,
        row_count: Option<i32>,
        error: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO nl_conversation_turns
               (conversation_id, turn_index, question, generated_sql,
                execution_time_ms, row_count, error)
               VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        )
        .bind(conversation_id)
        .bind(turn_index)
        .bind(question)
        .bind(generated_sql)
        .bind(execution_time_ms)
        .bind(row_count)
        .bind(error)
        .execute(&self.db)
        .await?;

        sqlx::query("UPDATE nl_conversations SET updated_at = NOW() WHERE id = $1")
            .bind(conversation_id)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    /// Verify that a conversation exists and belongs to the given project.
    pub async fn verify_ownership(
        &self,
        conversation_id: Uuid,
        project_id: Uuid,
    ) -> Result<bool, sqlx::Error> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM nl_conversations WHERE id = $1 AND project_id = $2)",
        )
        .bind(conversation_id)
        .bind(project_id)
        .fetch_one(&self.db)
        .await?;
        Ok(exists)
    }

    /// Load recent successful NL query pairs for few-shot prompting.
    pub async fn recent_successful_pairs(
        &self,
        project_id: Uuid,
        limit: i64,
    ) -> Result<Vec<HistoryEntry>, sqlx::Error> {
        let rows = sqlx::query(
            r#"SELECT t.question, t.generated_sql
               FROM nl_conversation_turns t
               JOIN nl_conversations c ON c.id = t.conversation_id
               WHERE c.project_id = $1 AND t.error IS NULL
               ORDER BY t.created_at DESC
               LIMIT $2"#,
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| HistoryEntry {
                question: r.get("question"),
                sql: r.get("generated_sql"),
            })
            .collect())
    }
}
