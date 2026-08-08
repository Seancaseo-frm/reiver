//! Shared utilities for the MongoDB connector.

/// Escape a string for ClickHouse SQL.
///
/// Handles:
/// - Backslashes
/// - Single quotes
/// - Null bytes
/// - Other control characters
pub fn escape_clickhouse_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\'' => result.push_str("\\'"),
            '\0' => result.push_str("\\0"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            // Handle other control characters (ASCII 0x01-0x1F except those above)
            c if c.is_control() => {
                // Use hex escape for other control characters
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("\\x{:02x}", byte));
                }
            }
            _ => result.push(c),
        }
    }
    
    result
}

/// Escape a ClickHouse identifier (column/table name).
pub fn escape_clickhouse_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_clickhouse_string() {
        assert_eq!(escape_clickhouse_string("normal"), "normal");
        assert_eq!(escape_clickhouse_string("it's"), "it\\'s");
        assert_eq!(escape_clickhouse_string("back\\slash"), "back\\\\slash");
        assert_eq!(escape_clickhouse_string("line\nbreak"), "line\\nbreak");
        assert_eq!(escape_clickhouse_string("null\0byte"), "null\\0byte");
        assert_eq!(escape_clickhouse_string("tab\there"), "tab\\there");
    }

    #[test]
    fn test_escape_clickhouse_identifier() {
        assert_eq!(escape_clickhouse_identifier("column"), "`column`");
        assert_eq!(escape_clickhouse_identifier("user__name"), "`user__name`");
        assert_eq!(escape_clickhouse_identifier("col`name"), "`col``name`");
    }
}
