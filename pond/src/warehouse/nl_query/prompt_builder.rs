//! Builds LLM prompts from warehouse catalog metadata.
//!
//! Formats table schemas, column types, and descriptions into a compact
//! schema context that helps the LLM generate accurate SQL queries.

use serde::{Deserialize, Serialize};

use crate::warehouse::catalog::types::CatalogEntry;
use super::conversation::{ConversationTurn, HistoryEntry};

/// System prompt template for SQL generation.
const SYSTEM_PROMPT: &str = r#"You are a SQL query generator for a data warehouse. Your job is to convert natural language questions into SQL queries.

Rules:
- Output ONLY the SQL query, nothing else. No explanations, no markdown, no code fences.
- Only generate SELECT statements. Never generate INSERT, UPDATE, DELETE, DROP, ALTER, or any other DDL/DML.
- Use the exact table and column names from the schema below. Do not invent table or column names.
- Use standard SQL syntax compatible with ClickHouse.
- When aggregating, always include a GROUP BY clause for non-aggregated columns.
- Use appropriate aliases for computed columns.
- Default to LIMIT 100 if the user doesn't specify a limit, to avoid returning too many rows.
- For date/time filtering, use ClickHouse date functions (toDate, toDateTime, now(), today()).

Available tables:
{schema_context}

Examples:
Q: "How many rows are in the events table?"
A: SELECT count(*) as total_rows FROM events

Q: "Show me the top 10 customers by revenue"
A: SELECT customer_id, SUM(amount) as total_revenue FROM orders GROUP BY customer_id ORDER BY total_revenue DESC LIMIT 10

Q: "What was the average order value last month?"
A: SELECT avg(amount) as avg_order_value FROM orders WHERE toDate(created_at) >= toStartOfMonth(today() - INTERVAL 1 MONTH) AND toDate(created_at) < toStartOfMonth(today())"#;

/// A chat message for the LLM API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// Schema context formatted for inclusion in the LLM prompt.
pub struct SchemaContext {
    pub formatted: String,
    pub table_count: usize,
}

/// Builds prompts for the text-to-SQL LLM pipeline.
pub struct PromptBuilder;

/// Maximum number of tables to include with full column details in the
/// schema context.
///
/// With an average of ~8 columns per table and ~60 characters per column line,
/// 50 tables ≈ 24KB of schema text ≈ ~6K tokens. This keeps the schema within
/// a reasonable fraction of the LLM's context window (typically 8K–128K tokens),
/// leaving room for the system prompt, user question, and generated SQL.
///
/// Beyond this threshold, the prompt switches to a compact format that lists
/// only table names and descriptions, relying on a future two-step approach
/// to fetch full schemas for relevant tables.
const MAX_TABLES_FULL_SCHEMA: usize = 50;

impl PromptBuilder {
    /// Build a compact schema context string from catalog entries.
    ///
    /// Formats each table with its source, row count estimate, and columns
    /// with types and descriptions.
    ///
    /// For large catalogs (50+ tables), only includes table names and
    /// descriptions in the schema context (without full column details)
    /// to stay within LLM context window limits.
    pub fn build_schema_context(entries: &[CatalogEntry]) -> SchemaContext {
        let table_count = entries.len();

        if table_count > MAX_TABLES_FULL_SCHEMA {
            Self::build_compact_schema_context(entries)
        } else {
            Self::build_full_schema_context(entries)
        }
    }

    /// Build full schema context with column details for each table.
    fn build_full_schema_context(entries: &[CatalogEntry]) -> SchemaContext {
        let mut lines = Vec::new();

        for entry in entries {
            // Format row count estimate (from freshness info)
            let row_count = entry
                .freshness
                .row_count_estimate
                .map(|n| format_row_count(n))
                .unwrap_or_else(|| "unknown".to_string());

            // Table header (source is already part of the qualified name)
            lines.push(format!(
                "Table: {}.{} (~{} rows)",
                entry.source_name, entry.table_name, row_count
            ));

            // Description if available
            if let Some(ref desc) = entry.description {
                if !desc.is_empty() {
                    lines.push(format!("  Description: {}", desc));
                }
            }

            // Columns
            let col_parts: Vec<String> = entry
                .schema
                .columns
                .iter()
                .map(|col| {
                    let type_name = &col.source_type_name;
                    if let Some(ref desc) = col.description {
                        if !desc.is_empty() {
                            return format!("{} ({}, \"{}\")", col.name, type_name, desc);
                        }
                    }
                    format!("{} ({})", col.name, type_name)
                })
                .collect();

            lines.push(format!("  Columns: {}", col_parts.join(", ")));
            lines.push(String::new()); // blank line between tables
        }

        SchemaContext {
            table_count: entries.len(),
            formatted: lines.join("\n"),
        }
    }

    /// Build a compact schema context for large catalogs (50+ tables).
    ///
    /// Only includes table names and descriptions, omitting column details
    /// to stay within LLM context window limits. The LLM can still generate
    /// reasonable queries by inferring column names from table context.
    fn build_compact_schema_context(entries: &[CatalogEntry]) -> SchemaContext {
        let mut lines = Vec::new();

        lines.push(format!(
            "NOTE: This project has {} tables. Showing table names and descriptions only (column details omitted for brevity).\n",
            entries.len()
        ));

        for entry in entries {
            let row_count = entry
                .freshness
                .row_count_estimate
                .map(|n| format_row_count(n))
                .unwrap_or_else(|| "unknown".to_string());

            let desc_part = entry
                .description
                .as_ref()
                .filter(|d| !d.is_empty())
                .map(|d| format!(" - {}", d))
                .unwrap_or_default();

            lines.push(format!(
                "- {}.{} (~{} rows){}",
                entry.source_name, entry.table_name, row_count, desc_part
            ));
        }

        SchemaContext {
            table_count: entries.len(),
            formatted: lines.join("\n"),
        }
    }

    /// Build the full prompt messages array for the LLM.
    ///
    /// If `error_context` is provided (for retry attempts), includes the
    /// previous failed SQL and error message so the LLM can self-correct.
    pub fn build_prompt(
        schema_context: &SchemaContext,
        question: &str,
        error_context: Option<&str>,
    ) -> Vec<ChatMessage> {
        Self::build_prompt_with_few_shot(schema_context, question, error_context, &[])
    }

    /// Build the prompt with optional few-shot examples from query history.
    pub fn build_prompt_with_few_shot(
        schema_context: &SchemaContext,
        question: &str,
        error_context: Option<&str>,
        history: &[HistoryEntry],
    ) -> Vec<ChatMessage> {
        let mut system_content =
            SYSTEM_PROMPT.replace("{schema_context}", &schema_context.formatted);

        let few_shot = Self::build_few_shot_examples(history);
        if !few_shot.is_empty() {
            system_content.push_str("\n\n");
            system_content.push_str(&few_shot);
        }

        let mut messages = vec![ChatMessage {
            role: "system".to_string(),
            content: system_content,
        }];

        if let Some(error) = error_context {
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: question.to_string(),
            });
            messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: "I generated an incorrect query. Let me fix it.".to_string(),
            });
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: format!(
                    "The previous attempt failed with this error:\n{}\n\nPlease generate a corrected SQL query for my original question: {}",
                    error, question
                ),
            });
        } else {
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: question.to_string(),
            });
        }

        messages
    }

    /// Build a conversational prompt that includes prior turns as chat messages.
    ///
    /// Prior question/SQL pairs are formatted as user/assistant message pairs,
    /// giving the LLM full context of the conversation history.
    pub fn build_conversational_prompt(
        schema_context: &SchemaContext,
        prior_turns: &[ConversationTurn],
        question: &str,
        error_context: Option<&str>,
        history: &[HistoryEntry],
    ) -> Vec<ChatMessage> {
        let mut system_content =
            SYSTEM_PROMPT.replace("{schema_context}", &schema_context.formatted);

        let few_shot = Self::build_few_shot_examples(history);
        if !few_shot.is_empty() {
            system_content.push_str("\n\n");
            system_content.push_str(&few_shot);
        }

        let mut messages = vec![ChatMessage {
            role: "system".to_string(),
            content: system_content,
        }];

        for turn in prior_turns {
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: turn.question.clone(),
            });
            messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: turn.generated_sql.clone(),
            });
        }

        if let Some(error) = error_context {
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: question.to_string(),
            });
            messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: "I generated an incorrect query. Let me fix it.".to_string(),
            });
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: format!(
                    "The previous attempt failed with this error:\n{}\n\nPlease generate a corrected SQL query for my original question: {}",
                    error, question
                ),
            });
        } else {
            messages.push(ChatMessage {
                role: "user".to_string(),
                content: question.to_string(),
            });
        }

        messages
    }

    /// Format recent successful NL query pairs as few-shot examples for the system prompt.
    pub fn build_few_shot_examples(history: &[HistoryEntry]) -> String {
        if history.is_empty() {
            return String::new();
        }

        let mut lines = vec!["Previously successful queries for this project:".to_string()];
        for entry in history {
            lines.push(format!("Q: \"{}\"", entry.question));
            lines.push(format!("A: {}", entry.sql));
            lines.push(String::new());
        }
        lines.join("\n")
    }
}

/// Format a row count estimate into a human-readable string.
fn format_row_count(count: i64) -> String {
    if count >= 1_000_000_000 {
        format!("{:.1}B", count as f64 / 1_000_000_000.0)
    } else if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K", count as f64 / 1_000.0)
    } else {
        count.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_row_count() {
        assert_eq!(format_row_count(0), "0");
        assert_eq!(format_row_count(500), "500");
        assert_eq!(format_row_count(1_500), "1.5K");
        assert_eq!(format_row_count(1_200_000), "1.2M");
        assert_eq!(format_row_count(2_500_000_000), "2.5B");
    }

    #[test]
    fn test_build_prompt_no_error() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let messages = PromptBuilder::build_prompt(&ctx, "How many orders?", None);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[1].role, "user");
        assert_eq!(messages[1].content, "How many orders?");
    }

    #[test]
    fn test_build_prompt_with_error_context() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let messages = PromptBuilder::build_prompt(
            &ctx,
            "How many orders?",
            Some("Column 'order_id' not found"),
        );
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[1].role, "user");
        assert_eq!(messages[2].role, "assistant");
        assert_eq!(messages[3].role, "user");
        assert!(messages[3].content.contains("Column 'order_id' not found"));
    }

    // --- Schema context tests ---

    fn make_test_entry(source: &str, table: &str) -> CatalogEntry {
        use crate::warehouse::types::TypedSchema;
        CatalogEntry {
            id: uuid::Uuid::new_v4(),
            project_id: uuid::Uuid::new_v4(),
            source_id: None,
            source_name: source.to_string(),
            table_name: table.to_string(),
            schema: TypedSchema {
                table_name: table.to_string(),
                columns: vec![],
                source_name: source.to_string(),
                updated_at: None,
            },
            description: None,
            tags: vec![],
            freshness: Default::default(),
            fulltext_columns: vec![],
            discovered_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn make_entry_with_columns(
        source: &str,
        table: &str,
        col_names: &[&str],
    ) -> CatalogEntry {
        use crate::warehouse::types::{TypedColumn, TypedSchema};
        let columns = col_names
            .iter()
            .map(|name| {
                TypedColumn::new(
                    *name,
                    &arrow::datatypes::DataType::Utf8,
                    true,
                    "String",
                    source,
                )
            })
            .collect();
        let mut entry = make_test_entry(source, table);
        entry.schema = TypedSchema {
            table_name: table.to_string(),
            columns,
            source_name: source.to_string(),
            updated_at: None,
        };
        entry
    }

    #[test]
    fn test_build_schema_context_full() {
        let entries = vec![
            make_entry_with_columns("db", "orders", &["id", "amount", "customer_id"]),
            make_entry_with_columns("db", "customers", &["id", "name"]),
        ];

        let ctx = PromptBuilder::build_schema_context(&entries);
        assert_eq!(ctx.table_count, 2);
        // Full context should include column names
        assert!(ctx.formatted.contains("id"));
        assert!(ctx.formatted.contains("amount"));
        assert!(ctx.formatted.contains("customer_id"));
        assert!(ctx.formatted.contains("db.orders"));
        assert!(ctx.formatted.contains("db.customers"));
    }

    #[test]
    fn test_build_schema_context_compact() {
        // Create 51 entries to trigger compact mode
        let entries: Vec<CatalogEntry> = (0..51)
            .map(|i| make_entry_with_columns("db", &format!("table_{}", i), &["id", "name"]))
            .collect();

        let ctx = PromptBuilder::build_schema_context(&entries);
        assert_eq!(ctx.table_count, 51);
        // Compact context should NOT include column details, only table names
        assert!(ctx.formatted.contains("table_0"));
        assert!(ctx.formatted.contains("table_50"));
        // Should include the NOTE about compact format
        assert!(ctx.formatted.contains("51 tables"));
        // Column names should NOT appear in the compact context
        // (the format uses "- source.table" not "Columns:")
        assert!(!ctx.formatted.contains("Columns:"));
    }

    #[test]
    fn test_build_schema_context_threshold_boundary() {
        // Exactly 50 tables -> full schema
        let entries_50: Vec<CatalogEntry> = (0..50)
            .map(|i| make_entry_with_columns("db", &format!("t_{}", i), &["col_a"]))
            .collect();
        let ctx_50 = PromptBuilder::build_schema_context(&entries_50);
        assert!(ctx_50.formatted.contains("Columns:"));
        assert!(!ctx_50.formatted.contains("NOTE:"));

        // 51 tables -> compact schema
        let entries_51: Vec<CatalogEntry> = (0..51)
            .map(|i| make_test_entry("db", &format!("t_{}", i)))
            .collect();
        let ctx_51 = PromptBuilder::build_schema_context(&entries_51);
        assert!(!ctx_51.formatted.contains("Columns:"));
        assert!(ctx_51.formatted.contains("NOTE:"));
    }

    #[test]
    fn test_full_schema_includes_row_count() {
        use crate::warehouse::catalog::types::FreshnessInfo;
        let mut entry = make_entry_with_columns("db", "events", &["id"]);
        entry.freshness = FreshnessInfo {
            row_count_estimate: Some(1_500),
            ..Default::default()
        };

        let ctx = PromptBuilder::build_schema_context(&[entry]);
        assert!(ctx.formatted.contains("~1.5K rows"));
    }

    #[test]
    fn test_compact_schema_includes_descriptions() {
        let entries: Vec<CatalogEntry> = (0..51)
            .map(|i| {
                let mut entry = make_test_entry("db", &format!("table_{}", i));
                if i == 0 {
                    entry.description = Some("Customer orders table".to_string());
                }
                entry
            })
            .collect();

        let ctx = PromptBuilder::build_schema_context(&entries);
        assert!(ctx.formatted.contains("Customer orders table"));
    }

    // ==================== Error Context Tests ====================

    #[test]
    fn test_error_context_includes_original_question_and_error() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let question = "Show me total revenue";
        let error = "Column 'revenue' not found in table 'orders'";
        let messages = PromptBuilder::build_prompt(&ctx, question, Some(error));

        // The retry message (last) should contain both the error and the original question
        let retry_msg = &messages[3].content;
        assert!(
            retry_msg.contains(error),
            "Retry message should contain the error text"
        );
        assert!(
            retry_msg.contains(question),
            "Retry message should contain the original question"
        );
    }

    #[test]
    fn test_multiple_retries_dont_accumulate() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let question = "Show me total revenue";

        // First retry with one error
        let messages1 = PromptBuilder::build_prompt(
            &ctx,
            question,
            Some("Error 1: syntax error at position 42"),
        );
        // Second retry with a different error
        let messages2 = PromptBuilder::build_prompt(
            &ctx,
            question,
            Some("Error 2: unknown column 'foo'"),
        );

        // Each build produces a clean message set; the second should NOT contain Error 1
        assert_eq!(messages1.len(), 4);
        assert_eq!(messages2.len(), 4);
        assert!(messages2[3].content.contains("Error 2"));
        assert!(
            !messages2[3].content.contains("Error 1"),
            "Second retry should not accumulate first error"
        );
    }

    #[test]
    fn test_error_context_with_special_characters() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let question = "Show me orders for customer 'O'Brien'";
        let error = "Syntax error near `'O'Brien'`\nat line 1:\nSELECT * FROM \"orders\" WHERE name = 'O''Brien'";
        let messages = PromptBuilder::build_prompt(&ctx, question, Some(error));

        // Should include the full error without mangling
        assert!(messages[3].content.contains("O'Brien"));
        assert!(messages[3].content.contains("Syntax error"));
    }

    #[test]
    fn test_error_context_with_long_error_message() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let question = "Count orders";
        // Build a 1000+ character error message
        let long_error = "x".repeat(1500);
        let messages = PromptBuilder::build_prompt(&ctx, question, Some(&long_error));

        // Should include the full long error without truncation or panic
        assert_eq!(messages.len(), 4);
        assert!(
            messages[3].content.contains(&long_error),
            "Long error should be included in full"
        );
    }

    #[test]
    fn test_no_error_context_produces_two_messages() {
        let ctx = SchemaContext {
            formatted: "Table: t\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let messages = PromptBuilder::build_prompt(&ctx, "Count rows", None);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[1].role, "user");
        // System prompt should contain schema
        assert!(messages[0].content.contains("Table: t"));
    }

    // ==================== Few-Shot Tests ====================

    #[test]
    fn test_few_shot_empty_history() {
        let result = PromptBuilder::build_few_shot_examples(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_few_shot_with_entries() {
        let history = vec![
            HistoryEntry {
                question: "How many orders?".to_string(),
                sql: "SELECT count(*) FROM orders".to_string(),
            },
            HistoryEntry {
                question: "Top customers".to_string(),
                sql: "SELECT customer_id, count(*) as c FROM orders GROUP BY customer_id ORDER BY c DESC LIMIT 10".to_string(),
            },
        ];
        let result = PromptBuilder::build_few_shot_examples(&history);
        assert!(result.contains("Previously successful queries"));
        assert!(result.contains("How many orders?"));
        assert!(result.contains("SELECT count(*) FROM orders"));
        assert!(result.contains("Top customers"));
    }

    #[test]
    fn test_prompt_with_few_shot_appends_to_system() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let history = vec![HistoryEntry {
            question: "count orders".to_string(),
            sql: "SELECT count(*) FROM orders".to_string(),
        }];
        let messages = PromptBuilder::build_prompt_with_few_shot(
            &ctx, "How many orders?", None, &history,
        );
        assert_eq!(messages.len(), 2);
        assert!(messages[0].content.contains("Previously successful queries"));
        assert!(messages[0].content.contains("count orders"));
    }

    #[test]
    fn test_prompt_with_empty_few_shot_matches_original() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let with_few_shot = PromptBuilder::build_prompt_with_few_shot(
            &ctx, "How many orders?", None, &[],
        );
        let without = PromptBuilder::build_prompt(&ctx, "How many orders?", None);
        assert_eq!(with_few_shot.len(), without.len());
        assert_eq!(with_few_shot[0].content, without[0].content);
        assert_eq!(with_few_shot[1].content, without[1].content);
    }

    // ==================== Conversational Prompt Tests ====================

    #[test]
    fn test_conversational_prompt_includes_prior_turns() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let prior_turns = vec![
            ConversationTurn {
                turn_index: 0,
                question: "How many orders?".to_string(),
                generated_sql: "SELECT count(*) FROM orders".to_string(),
                execution_time_ms: Some(42),
                row_count: Some(1),
                error: None,
                created_at: chrono::Utc::now(),
            },
        ];
        let messages = PromptBuilder::build_conversational_prompt(
            &ctx, &prior_turns, "Show average amount", None, &[],
        );
        // system + (user+assistant for prior turn) + user for new question
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[1].role, "user");
        assert_eq!(messages[1].content, "How many orders?");
        assert_eq!(messages[2].role, "assistant");
        assert_eq!(messages[2].content, "SELECT count(*) FROM orders");
        assert_eq!(messages[3].role, "user");
        assert_eq!(messages[3].content, "Show average amount");
    }

    #[test]
    fn test_conversational_prompt_with_error_context() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let prior_turns = vec![ConversationTurn {
            turn_index: 0,
            question: "How many orders?".to_string(),
            generated_sql: "SELECT count(*) FROM orders".to_string(),
            execution_time_ms: Some(42),
            row_count: Some(1),
            error: None,
            created_at: chrono::Utc::now(),
        }];
        let messages = PromptBuilder::build_conversational_prompt(
            &ctx, &prior_turns, "Show average amount",
            Some("column 'amount' not found"), &[],
        );
        // system + prior(user+assistant) + user + assistant(error ack) + user(retry)
        assert_eq!(messages.len(), 6);
        assert!(messages[5].content.contains("column 'amount' not found"));
    }

    #[test]
    fn test_conversational_prompt_multiple_turns() {
        let ctx = SchemaContext {
            formatted: "Table: t\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let prior_turns = vec![
            ConversationTurn {
                turn_index: 0,
                question: "Q1".to_string(),
                generated_sql: "SQL1".to_string(),
                execution_time_ms: None,
                row_count: None,
                error: None,
                created_at: chrono::Utc::now(),
            },
            ConversationTurn {
                turn_index: 1,
                question: "Q2".to_string(),
                generated_sql: "SQL2".to_string(),
                execution_time_ms: None,
                row_count: None,
                error: None,
                created_at: chrono::Utc::now(),
            },
        ];
        let messages = PromptBuilder::build_conversational_prompt(
            &ctx, &prior_turns, "Q3", None, &[],
        );
        // system + 2*(user+assistant) + user
        assert_eq!(messages.len(), 6);
        assert_eq!(messages[1].content, "Q1");
        assert_eq!(messages[2].content, "SQL1");
        assert_eq!(messages[3].content, "Q2");
        assert_eq!(messages[4].content, "SQL2");
        assert_eq!(messages[5].content, "Q3");
    }

    #[test]
    fn test_conversational_prompt_with_few_shot() {
        let ctx = SchemaContext {
            formatted: "Table: orders\n  Columns: id (Int64)".to_string(),
            table_count: 1,
        };
        let prior_turns = vec![ConversationTurn {
            turn_index: 0,
            question: "Count rows".to_string(),
            generated_sql: "SELECT count(*) FROM orders".to_string(),
            execution_time_ms: None,
            row_count: None,
            error: None,
            created_at: chrono::Utc::now(),
        }];
        let history = vec![HistoryEntry {
            question: "historical q".to_string(),
            sql: "historical sql".to_string(),
        }];
        let messages = PromptBuilder::build_conversational_prompt(
            &ctx, &prior_turns, "Next question", None, &history,
        );
        assert!(messages[0].content.contains("Previously successful queries"));
        assert!(messages[0].content.contains("historical q"));
    }
}
