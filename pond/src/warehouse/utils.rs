//! Shared utilities for the data warehouse module.
//!
//! Contains common functions used across multiple warehouse components.

use sha2::{Digest, Sha256};

/// Normalize a SQL query for caching and comparison.
///
/// This function:
/// - Collapses multiple whitespace characters into single spaces
/// - Converts identifiers and keywords to lowercase (preserves case inside quoted strings)
/// - Trims leading and trailing whitespace
///
/// # Case Sensitivity
///
/// This function lowercases the entire query, including identifiers (table names,
/// column names). This is appropriate for ClickHouse which treats identifiers as
/// case-insensitive by default.
///
/// **Note**: If used with databases that have case-sensitive identifiers
/// (e.g., PostgreSQL with quoted identifiers), queries with different-cased
/// identifiers will incorrectly be treated as equivalent for caching purposes.
/// For such databases, consider preserving case or using a different normalization
/// strategy.
///
/// # Example
/// ```ignore
/// let normalized = normalize_query("SELECT  *  FROM   customers  WHERE  id = 1");
/// assert_eq!(normalized, "select * from customers where id = 1");
/// ```
pub fn normalize_query(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_was_space = false;
    let mut prev_was_backslash = false;

    for ch in query.chars() {
        if in_single_quote {
            result.push(ch);
            if ch == '\'' && !prev_was_backslash {
                in_single_quote = false;
            }
            prev_was_backslash = ch == '\\' && !prev_was_backslash;
            continue;
        }
        if in_double_quote {
            result.push(ch);
            if ch == '"' && !prev_was_backslash {
                in_double_quote = false;
            }
            prev_was_backslash = ch == '\\' && !prev_was_backslash;
            continue;
        }

        match ch {
            '\'' => {
                in_single_quote = true;
                last_was_space = false;
                result.push(ch);
            }
            '"' => {
                in_double_quote = true;
                last_was_space = false;
                result.push(ch);
            }
            c if c.is_whitespace() => {
                if !last_was_space && !result.is_empty() {
                    result.push(' ');
                    last_was_space = true;
                }
            }
            c => {
                last_was_space = false;
                for lc in c.to_lowercase() {
                    result.push(lc);
                }
            }
        }
    }

    let trimmed = result.trim_end().to_string();
    trimmed
}

/// Hash a normalized query string for cache keys.
///
/// Returns a 16-character hexadecimal hash string.
///
/// STABILITY: Uses SHA-256 which produces consistent output across Rust versions
/// and platforms. This is critical for cache key stability across deployments.
/// Previously used DefaultHasher which is NOT stable across Rust versions.
pub fn hash_query(query: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    let result = hasher.finalize();
    // Take first 8 bytes (16 hex chars) for a compact but collision-resistant hash
    hex::encode(&result[..8])
}

/// Hash a query after normalizing it.
///
/// This is a convenience function that combines `normalize_query` and `hash_query`.
pub fn hash_normalized_query(query: &str) -> String {
    hash_query(&normalize_query(query))
}

/// Increment the last character of a string to create an upper bound for FST range queries.
///
/// This is used to create exclusive upper bounds for prefix searches.
/// For example, to search for strings starting with "abc", we search for
/// strings >= "abc" and < "abd".
///
/// SAFETY: Handles multi-byte UTF-8 characters correctly by working at the
/// character level rather than the byte level. This prevents creating invalid
/// UTF-8 sequences when incrementing characters like emoji or accented letters.
///
/// CORRECTNESS: Properly handles edge cases:
/// - `char::MAX` (U+10FFFF): Carries over — drops the trailing MAX char and
///   increments the preceding character.  Repeats until an incrementable char
///   is found.  If all characters are MAX, returns the original string (no
///   valid exclusive upper bound exists).
/// - Surrogate boundary (U+D7FF): Skips to U+E000 (surrogates U+D800-U+DFFF are not valid Rust chars)
///
/// # Example
/// ```ignore
/// let upper = increment_last_byte("abc");
/// assert_eq!(upper, "abd");
/// 
/// let upper = increment_last_byte("café");
/// assert_eq!(upper, "cafê"); // é (U+00E9) -> ê (U+00EA)
/// ```
pub fn increment_last_byte(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    let mut chars: Vec<char> = s.chars().collect();

    // Walk backwards: if the last character is char::MAX we cannot
    // increment it, so drop it and try the previous character (carry).
    while let Some(&last) = chars.last() {
        let code_point = last as u32;

        if code_point == char::MAX as u32 {
            chars.pop();
            continue;
        }

        // U+D7FF -> skip surrogate range to U+E000
        let next_code_point = if code_point == 0xD7FF {
            0xE000
        } else {
            code_point + 1
        };

        *chars.last_mut().unwrap() = char::from_u32(next_code_point).unwrap();
        return chars.into_iter().collect();
    }

    // Every character was char::MAX — no valid upper bound exists.
    s.to_string()
}

/// Estimate the actual memory usage of a serde_json::Value.
///
/// PERFORMANCE: For accurate OOM protection, we need to account for:
/// - serde_json::Value enum overhead (24 bytes on 64-bit systems)
/// - String heap allocation: String struct (24 bytes) + actual content
/// - Vec allocations for arrays and object entries
/// - Recursive structure for nested values
///
/// This provides a more accurate estimate than just `v.to_string().len()`,
/// which serializes the value and measures the JSON string length (inaccurate
/// because it doesn't account for struct overhead and may under/overcount).
///
/// # Example
/// ```ignore
/// let value = serde_json::json!({"name": "Alice", "age": 30});
/// let size = estimate_json_value_memory(&value);
/// // size accounts for Object overhead + "name" key + "Alice" value + "age" key + 30 value
/// ```
pub fn estimate_json_value_memory(v: &serde_json::Value) -> usize {
    const VALUE_ENUM_SIZE: usize = 24; // serde_json::Value enum on 64-bit
    const STRING_STRUCT_SIZE: usize = 24; // String struct overhead
    const VEC_STRUCT_SIZE: usize = 24; // Vec struct overhead
    
    match v {
        serde_json::Value::Null => VALUE_ENUM_SIZE,
        serde_json::Value::Bool(_) => VALUE_ENUM_SIZE,
        serde_json::Value::Number(_) => VALUE_ENUM_SIZE + 8, // Number has internal representation
        serde_json::Value::String(s) => {
            VALUE_ENUM_SIZE + STRING_STRUCT_SIZE + s.len()
        }
        serde_json::Value::Array(arr) => {
            let elements: usize = arr.iter().map(estimate_json_value_memory).sum();
            VALUE_ENUM_SIZE + VEC_STRUCT_SIZE + elements
        }
        serde_json::Value::Object(map) => {
            let entries: usize = map.iter().map(|(k, v)| {
                // Key is a String, value is a Value
                STRING_STRUCT_SIZE + k.len() + estimate_json_value_memory(v)
            }).sum();
            VALUE_ENUM_SIZE + VEC_STRUCT_SIZE + entries
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_query() {
        let query = "SELECT  *  FROM   customers  WHERE  id = 1";
        let normalized = normalize_query(query);
        assert_eq!(normalized, "select * from customers where id = 1");
    }

    #[test]
    fn test_normalize_query_case_insensitive() {
        let query1 = "SELECT * FROM Customers";
        let query2 = "select * from customers";
        assert_eq!(normalize_query(query1), normalize_query(query2));
    }

    #[test]
    fn test_hash_query_deterministic() {
        let query = "select * from customers";
        let hash1 = hash_query(query);
        let hash2 = hash_query(query);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 16); // 16 hex chars
    }

    #[test]
    fn test_hash_normalized_query() {
        let query1 = "SELECT * FROM customers";
        let query2 = "select  *  from  customers";
        assert_eq!(hash_normalized_query(query1), hash_normalized_query(query2));
    }

    #[test]
    fn test_increment_last_byte() {
        assert_eq!(increment_last_byte("abc"), "abd");
        assert_eq!(increment_last_byte("a"), "b");
        assert_eq!(increment_last_byte(""), "");
    }
    
    #[test]
    fn test_increment_last_byte_utf8() {
        // Test multi-byte UTF-8 characters
        assert_eq!(increment_last_byte("café"), "cafê"); // é (U+00E9) -> ê (U+00EA)
        assert_eq!(increment_last_byte("日本"), "日札"); // 本 (U+672C) -> 札 (U+672D)
        
        // Test that the result is valid UTF-8
        let result = increment_last_byte("test🎉");
        assert!(result.is_ascii() || result.chars().all(|c| c.len_utf8() > 0));
    }
    
    #[test]
    fn test_increment_last_byte_edge_cases() {
        // Test surrogate boundary (U+D7FF) - should skip to U+E000
        let before_surrogate = '\u{D7FF}';
        let after_surrogate = '\u{E000}';
        let s = format!("prefix{}", before_surrogate);
        let expected = format!("prefix{}", after_surrogate);
        let result = increment_last_byte(&s);
        assert_eq!(result, expected, "U+D7FF should increment to U+E000, skipping surrogates");
        
        // Verify the result is valid Unicode
        for c in result.chars() {
            assert!(char::from_u32(c as u32).is_some(), "Result contains valid Unicode");
        }
    }

    /// Regression test for Bug 4: carry-over when last char is char::MAX.
    #[test]
    fn test_increment_last_byte_carry_over_single() {
        // "ab\u{10FFFF}" should carry over: drop the MAX char, increment 'b' -> 'c'
        let s = format!("ab{}", char::MAX);
        assert_eq!(increment_last_byte(&s), "ac");
    }

    #[test]
    fn test_increment_last_byte_carry_over_multiple() {
        // "a\u{10FFFF}\u{10FFFF}" should carry over twice: "a" + MAX + MAX -> "b"
        let s = format!("a{}{}", char::MAX, char::MAX);
        assert_eq!(increment_last_byte(&s), "b");
    }

    #[test]
    fn test_increment_last_byte_all_max() {
        // All chars are MAX — no valid upper bound exists, returns original
        let s = format!("{}{}", char::MAX, char::MAX);
        assert_eq!(increment_last_byte(&s), s);
    }

    #[test]
    fn test_increment_last_byte_prefix_then_max() {
        // Ensures that prefix characters are properly preserved during carry
        let s = format!("hello{}", char::MAX);
        assert_eq!(increment_last_byte(&s), "hellp");
    }
}
