//! Shared utilities for the SQL Server connector.

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
            // Handle other control characters
            c if c.is_control() => {
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

/// Escape a string for SQL Server.
pub fn escape_sqlserver_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a SQL Server identifier.
pub fn escape_sqlserver_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Convert a binary LSN to a hex string.
pub fn lsn_to_hex(lsn: &[u8]) -> String {
    hex::encode(lsn)
}

/// Convert a hex string to a binary LSN.
pub fn hex_to_lsn(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex)
}

/// Format a binary LSN for display (SQL Server format: 0x00000000:00000000:0000).
pub fn format_lsn(lsn: &[u8]) -> String {
    if lsn.len() != 10 {
        return format!("0x{}", hex::encode(lsn));
    }

    format!(
        "0x{:08x}:{:08x}:{:04x}",
        u32::from_be_bytes([lsn[0], lsn[1], lsn[2], lsn[3]]),
        u32::from_be_bytes([lsn[4], lsn[5], lsn[6], lsn[7]]),
        u16::from_be_bytes([lsn[8], lsn[9]])
    )
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

    #[test]
    fn test_escape_sqlserver_string() {
        assert_eq!(escape_sqlserver_string("normal"), "normal");
        assert_eq!(escape_sqlserver_string("it's"), "it''s");
        assert_eq!(escape_sqlserver_string("a'b'c"), "a''b''c");
    }

    #[test]
    fn test_escape_sqlserver_identifier() {
        assert_eq!(escape_sqlserver_identifier("column"), "[column]");
        assert_eq!(escape_sqlserver_identifier("user_name"), "[user_name]");
        assert_eq!(escape_sqlserver_identifier("col]name"), "[col]]name]");
    }

    #[test]
    fn test_lsn_conversion() {
        let lsn = vec![0x00, 0x00, 0x00, 0x1A, 0x00, 0x00, 0x00, 0x50, 0x00, 0x01];
        let hex = lsn_to_hex(&lsn);
        assert_eq!(hex, "0000001a000000500001");

        let back = hex_to_lsn(&hex).unwrap();
        assert_eq!(back, lsn);
    }

    #[test]
    fn test_format_lsn() {
        let lsn = vec![0x00, 0x00, 0x00, 0x1A, 0x00, 0x00, 0x00, 0x50, 0x00, 0x01];
        let formatted = format_lsn(&lsn);
        assert!(formatted.starts_with("0x"));
    }
}
