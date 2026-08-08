//! Tokenization utilities and shared constants for full-text search indexing.
//!
//! Token FSTs are integrated into the skip index pipeline (see
//! `extract_token_values` in `job_worker.rs`). This module provides the
//! shared tokenizer and the FTS column prefix constant.

/// Prefix used to distinguish fulltext token FST entries from value FST entries
/// in the skip index. Token entries use `__fts__:{column_name}` as the column name.
pub const FTS_COLUMN_PREFIX: &str = "__fts__:";

/// Tokenize a string value into individual search tokens.
///
/// Splits on non-alphanumeric characters (preserving underscores),
/// lowercases, and filters out tokens shorter than 2 characters.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| s.len() >= 2)
        .map(|s| s.to_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        let tokens = tokenize("Hello, world! This is a test-123.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"this".to_string()));
        assert!(tokens.contains(&"test".to_string()));
        assert!(tokens.contains(&"123".to_string()));
        assert!(tokens.contains(&"is".to_string()));
        assert!(!tokens.iter().any(|t| t == "a"));
    }

    #[test]
    fn test_tokenize_underscore_preserved() {
        let tokens = tokenize("user_id timeout_error");
        assert!(tokens.contains(&"user_id".to_string()));
        assert!(tokens.contains(&"timeout_error".to_string()));
    }
}
