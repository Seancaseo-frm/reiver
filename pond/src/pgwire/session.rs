//! Session command handler for the pgwire server.
//!
//! Handles SET, SHOW, transaction control (BEGIN/COMMIT/ROLLBACK), and
//! miscellaneous session commands (DISCARD, DEALLOCATE, RESET) that BI tools
//! and JDBC/ODBC drivers send during connection initialization.
//!
//! These commands never reach ClickHouse -- they are handled entirely within
//! the pgwire layer with appropriate Postgres-compatible responses.

/// Session parameter keys that are seeded on new connections.
///
/// Used by `auth.rs` during connection startup and by `default_value_for()`
/// to provide Postgres-compatible defaults.
pub const DEFAULT_SESSION_KEYS: &[&str] = &[
    "server_version",
    "server_encoding",
    "client_encoding",
    "datestyle",
    "timezone",
    "standard_conforming_strings",
    "integer_datetimes",
    "intervalstyle",
    "is_superuser",
    "search_path",
    "transaction_isolation",
    "extra_float_digits",
    "application_name",
    "default_transaction_read_only",
];

/// Get the default value for a session parameter.
///
/// This is the single source of truth for Postgres-compatible session defaults.
/// Used when SHOW queries a parameter that hasn't been explicitly SET
/// on this connection, and when seeding defaults during auth.
pub fn default_value_for(key: &str) -> Option<&'static str> {
    match key {
        "server_version" => Some("16.6"),
        "server_encoding" => Some("UTF8"),
        "client_encoding" => Some("UTF8"),
        "datestyle" => Some("ISO, MDY"),
        "timezone" => Some("UTC"),
        "standard_conforming_strings" => Some("on"),
        "integer_datetimes" => Some("on"),
        "intervalstyle" => Some("postgres"),
        "is_superuser" => Some("off"),
        "search_path" => Some("\"$user\", public"),
        "transaction_isolation" => Some("read committed"),
        "extra_float_digits" => Some("3"),
        "application_name" => Some(""),
        "default_transaction_read_only" => Some("on"),
        _ => None,
    }
}

/// Classification of an incoming SQL statement for routing purposes.
#[derive(Debug)]
pub enum QueryClass {
    /// SET key = value -- store in session, return CommandComplete("SET")
    Set { key: String, value: String },
    /// SHOW key -- return single-row result with value from session
    Show { key: String },
    /// SHOW ALL -- return multi-row result with all session parameters
    ShowAll,
    /// BEGIN / START TRANSACTION -- no-op, return CommandComplete
    Begin,
    /// COMMIT / END -- no-op, return CommandComplete
    Commit,
    /// ROLLBACK / ABORT -- no-op, return CommandComplete
    Rollback,
    /// SAVEPOINT name -- no-op
    Savepoint,
    /// RELEASE SAVEPOINT name -- no-op
    Release,
    /// DISCARD ALL / DEALLOCATE ALL / CLOSE ALL / RESET ALL -- no-op
    DiscardAll,
    /// RESET variable -- reset to default
    Reset { key: String },
    /// Query that targets pg_catalog or information_schema tables
    CatalogQuery,
    /// Regular data query -- route to ClickHouse
    DataQuery,
}

/// Attempt to classify a SQL string as a session command.
///
/// Returns `Some(QueryClass)` for session commands, `None` if the query
/// needs further AST analysis (catalog vs data routing).
pub fn classify_session_command(sql: &str) -> Option<QueryClass> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Normalize to uppercase for keyword matching, but keep original for values
    let upper = trimmed.to_ascii_uppercase();

    // --- SET ---
    if upper.starts_with("SET ") {
        return parse_set(trimmed);
    }

    // --- SHOW ---
    if upper.starts_with("SHOW ") {
        let key = trimmed[5..].trim().trim_end_matches(';').trim();
        if key.eq_ignore_ascii_case("ALL") {
            return Some(QueryClass::ShowAll);
        }
        let key = key.to_ascii_lowercase();
        let key = match key.as_str() {
            "transaction isolation level" => "transaction_isolation".to_owned(),
            _ => key,
        };
        return Some(QueryClass::Show { key });
    }

    // --- Transaction control ---
    if upper.starts_with("BEGIN")
        || upper.starts_with("START TRANSACTION")
        || upper.starts_with("START WORK")
    {
        return Some(QueryClass::Begin);
    }
    if upper.starts_with("COMMIT")
        || upper.starts_with("END")
            && !upper.starts_with("ENDFOR")
            && !upper.starts_with("ENDIF")
    {
        return Some(QueryClass::Commit);
    }
    if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
        return Some(QueryClass::Rollback);
    }
    if upper.starts_with("SAVEPOINT ") {
        return Some(QueryClass::Savepoint);
    }
    if upper.starts_with("RELEASE ") {
        return Some(QueryClass::Release);
    }

    // --- DISCARD / DEALLOCATE / CLOSE / RESET ---
    if upper.starts_with("DISCARD ") || upper == "DISCARD" {
        return Some(QueryClass::DiscardAll);
    }
    if upper.starts_with("DEALLOCATE ") || upper == "DEALLOCATE" {
        return Some(QueryClass::DiscardAll);
    }
    if upper.starts_with("CLOSE ") || upper == "CLOSE" {
        return Some(QueryClass::DiscardAll);
    }
    if upper.starts_with("RESET ") {
        let key = trimmed[6..].trim().trim_end_matches(';').trim();
        if key.eq_ignore_ascii_case("ALL") {
            return Some(QueryClass::DiscardAll);
        }
        return Some(QueryClass::Reset {
            key: key.to_ascii_lowercase(),
        });
    }

    // --- LISTEN / UNLISTEN (used by some drivers) ---
    if upper.starts_with("LISTEN ") || upper.starts_with("UNLISTEN ") {
        return Some(QueryClass::DiscardAll); // no-op
    }

    None
}

/// Parse a SET statement into key/value.
fn parse_set(sql: &str) -> Option<QueryClass> {
    // Formats: SET key = value, SET key TO value, SET LOCAL key = value,
    //          SET SESSION key = value
    let rest = sql[4..].trim().trim_end_matches(';').trim();

    // Skip LOCAL / SESSION prefix
    let rest = if rest
        .to_ascii_uppercase()
        .starts_with("LOCAL ")
    {
        rest[6..].trim()
    } else if rest.to_ascii_uppercase().starts_with("SESSION ") {
        rest[8..].trim()
    } else {
        rest
    };

    // Handle special SET TIME ZONE syntax (no = or TO)
    let upper_rest = rest.to_ascii_uppercase();
    if upper_rest.starts_with("TIME ZONE ") {
        let value = rest[10..].trim().trim_matches('\'').trim_matches('"').to_owned();
        return Some(QueryClass::Set {
            key: "timezone".to_ascii_lowercase(),
            value,
        });
    }

    // Handle SET SESSION CHARACTERISTICS AS TRANSACTION ...
    if upper_rest.starts_with("CHARACTERISTICS ") {
        return Some(QueryClass::DiscardAll); // no-op, we don't support transactions
    }

    // Split on = or TO
    let (key, value) = if let Some(eq_pos) = rest.find('=') {
        let k = rest[..eq_pos].trim();
        let v = rest[eq_pos + 1..].trim();
        (k, v)
    } else if let Some(to_pos) = rest
        .to_ascii_uppercase()
        .find(" TO ")
    {
        let k = rest[..to_pos].trim();
        let v = rest[to_pos + 4..].trim();
        (k, v)
    } else {
        return Some(QueryClass::DiscardAll); // malformed SET, treat as no-op
    };

    // Strip one matching pair of enclosing quotes from value
    let value = if (value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2)
        || (value.starts_with('"') && value.ends_with('"') && value.len() >= 2)
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    };

    Some(QueryClass::Set {
        key: key.to_ascii_lowercase(),
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_set_equals() {
        match classify_session_command("SET client_encoding = 'UTF8'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "client_encoding");
                assert_eq!(value, "UTF8");
            }
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_set_to() {
        match classify_session_command("SET DateStyle TO 'ISO'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "datestyle");
                assert_eq!(value, "ISO");
            }
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_show() {
        match classify_session_command("SHOW server_version;") {
            Some(QueryClass::Show { key }) => {
                assert_eq!(key, "server_version");
            }
            other => panic!("Expected Show, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_begin() {
        assert!(matches!(
            classify_session_command("BEGIN"),
            Some(QueryClass::Begin)
        ));
        assert!(matches!(
            classify_session_command("START TRANSACTION"),
            Some(QueryClass::Begin)
        ));
    }

    #[test]
    fn test_classify_commit() {
        assert!(matches!(
            classify_session_command("COMMIT"),
            Some(QueryClass::Commit)
        ));
        assert!(matches!(
            classify_session_command("END"),
            Some(QueryClass::Commit)
        ));
    }

    #[test]
    fn test_classify_rollback() {
        assert!(matches!(
            classify_session_command("ROLLBACK"),
            Some(QueryClass::Rollback)
        ));
    }

    #[test]
    fn test_classify_discard() {
        assert!(matches!(
            classify_session_command("DISCARD ALL"),
            Some(QueryClass::DiscardAll)
        ));
        assert!(matches!(
            classify_session_command("DEALLOCATE ALL"),
            Some(QueryClass::DiscardAll)
        ));
        assert!(matches!(
            classify_session_command("RESET ALL"),
            Some(QueryClass::DiscardAll)
        ));
    }

    #[test]
    fn test_classify_show_all() {
        assert!(matches!(
            classify_session_command("SHOW ALL"),
            Some(QueryClass::ShowAll)
        ));
        assert!(matches!(
            classify_session_command("SHOW ALL;"),
            Some(QueryClass::ShowAll)
        ));
    }

    #[test]
    fn test_classify_data_query() {
        assert!(classify_session_command("SELECT * FROM orders").is_none());
    }

    #[test]
    fn test_default_value_for_known_keys() {
        assert_eq!(default_value_for("server_version"), Some("16.6"));
        assert_eq!(default_value_for("client_encoding"), Some("UTF8"));
        assert_eq!(default_value_for("timezone"), Some("UTC"));
    }

    #[test]
    fn test_default_value_for_unknown_key() {
        assert_eq!(default_value_for("nonexistent_param"), None);
    }

    #[test]
    fn test_session_reports_read_only() {
        assert_eq!(
            default_value_for("default_transaction_read_only"),
            Some("on"),
            "Must report read-only since all writes are rejected"
        );
    }

    #[test]
    fn test_session_reports_not_superuser() {
        assert_eq!(
            default_value_for("is_superuser"),
            Some("off"),
            "Must not claim superuser for a project-scoped read-only connection"
        );
    }

    #[test]
    fn test_all_default_keys_have_values() {
        // Every key in DEFAULT_SESSION_KEYS must have a default value
        for key in DEFAULT_SESSION_KEYS {
            assert!(
                default_value_for(key).is_some(),
                "DEFAULT_SESSION_KEYS contains '{}' but default_value_for returns None for it",
                key
            );
        }
    }

    // ── SET TIME ZONE special syntax ──

    #[test]
    fn test_set_time_zone_utc() {
        match classify_session_command("SET TIME ZONE 'UTC'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "UTC");
            }
            other => panic!("Expected Set for TIME ZONE 'UTC', got {:?}", other),
        }
    }

    #[test]
    fn test_set_time_zone_named() {
        match classify_session_command("SET TIME ZONE 'America/New_York'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "America/New_York");
            }
            other => panic!("Expected Set for TIME ZONE 'America/New_York', got {:?}", other),
        }
    }

    #[test]
    fn test_set_time_zone_double_quoted() {
        match classify_session_command("SET TIME ZONE \"Europe/London\"") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "Europe/London");
            }
            other => panic!("Expected Set for TIME ZONE \"Europe/London\", got {:?}", other),
        }
    }

    // ── Additional session command classification tests ──

    #[test]
    fn test_classify_reset_single_key() {
        match classify_session_command("RESET timezone") {
            Some(QueryClass::Reset { key }) => {
                assert_eq!(key, "timezone");
            }
            other => panic!("Expected Reset, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_listen_unlisten() {
        assert!(matches!(
            classify_session_command("LISTEN foo"),
            Some(QueryClass::DiscardAll)
        ));
        assert!(matches!(
            classify_session_command("UNLISTEN *"),
            Some(QueryClass::DiscardAll)
        ));
    }

    #[test]
    fn test_classify_savepoint() {
        assert!(matches!(
            classify_session_command("SAVEPOINT sp1"),
            Some(QueryClass::Savepoint)
        ));
    }

    #[test]
    fn test_classify_release() {
        assert!(matches!(
            classify_session_command("RELEASE sp1"),
            Some(QueryClass::Release)
        ));
    }

    #[test]
    fn test_classify_set_local() {
        match classify_session_command("SET LOCAL client_encoding = 'UTF8'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "client_encoding");
                assert_eq!(value, "UTF8");
            }
            other => panic!("Expected Set for SET LOCAL, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_set_session() {
        match classify_session_command("SET SESSION timezone = 'US/Pacific'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "US/Pacific");
            }
            other => panic!("Expected Set for SET SESSION, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_abort() {
        assert!(matches!(
            classify_session_command("ABORT"),
            Some(QueryClass::Rollback)
        ));
    }

    #[test]
    fn test_classify_close_all() {
        assert!(matches!(
            classify_session_command("CLOSE ALL"),
            Some(QueryClass::DiscardAll)
        ));
    }

    #[test]
    fn test_classify_case_insensitive() {
        // Lowercase SET should still work
        match classify_session_command("set timezone = 'utc'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "utc");
            }
            other => panic!("Expected Set for lowercase 'set', got {:?}", other),
        }
    }

    #[test]
    fn test_classify_set_value_with_comma() {
        match classify_session_command("SET search_path = 'public, myschema'") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "search_path");
                assert_eq!(value, "public, myschema");
            }
            other => panic!("Expected Set with comma in value, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_end_vs_endfor() {
        // "END" should be Commit
        assert!(matches!(
            classify_session_command("END"),
            Some(QueryClass::Commit)
        ));
        // "ENDFOR" should NOT match as a session command
        assert!(
            classify_session_command("ENDFOR").is_none(),
            "ENDFOR should not be classified as a session command"
        );
    }

    // ── Corner case tests ──

    #[test]
    fn test_classify_endif_not_commit() {
        // "ENDIF" should NOT match as Commit (operator precedence guard)
        assert!(
            classify_session_command("ENDIF").is_none(),
            "ENDIF should not be classified as a session command"
        );
    }

    #[test]
    fn test_classify_empty_string() {
        assert!(
            classify_session_command("").is_none(),
            "Empty string should return None"
        );
    }

    #[test]
    fn test_classify_whitespace_only() {
        assert!(
            classify_session_command("   ").is_none(),
            "Whitespace-only string should return None"
        );
    }

    #[test]
    fn test_classify_set_no_value() {
        // SET with no = or TO falls through to DiscardAll
        assert!(matches!(
            classify_session_command("SET timezone"),
            Some(QueryClass::DiscardAll)
        ));
    }

    #[test]
    fn test_classify_set_empty_value() {
        match classify_session_command("SET timezone = ''") {
            Some(QueryClass::Set { key, value }) => {
                assert_eq!(key, "timezone");
                assert_eq!(value, "");
            }
            other => panic!("Expected Set with empty value, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_show_trailing_whitespace() {
        match classify_session_command("SHOW timezone  ;  ") {
            Some(QueryClass::Show { key }) => {
                assert_eq!(key, "timezone");
            }
            other => panic!("Expected Show with trimmed key, got {:?}", other),
        }
    }

    #[test]
    fn test_classify_start_work() {
        assert!(matches!(
            classify_session_command("START WORK"),
            Some(QueryClass::Begin)
        ));
    }

    #[test]
    fn test_classify_deallocate_specific() {
        assert!(matches!(
            classify_session_command("DEALLOCATE stmt_name"),
            Some(QueryClass::DiscardAll)
        ));
    }

    #[test]
    fn test_set_strips_only_one_pair_of_quotes() {
        match classify_session_command("SET search_path = '''inner'''") {
            Some(QueryClass::Set { value, .. }) => {
                assert_eq!(
                    value, "''inner''",
                    "Only one pair of enclosing quotes should be stripped"
                );
            }
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_set_double_quoted_value() {
        match classify_session_command("SET search_path = \"\\\"$user\\\", public\"") {
            Some(QueryClass::Set { value, .. }) => {
                assert_eq!(
                    value, "\\\"$user\\\", public",
                    "Double-quoted value should have one pair stripped"
                );
            }
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_set_unquoted_value() {
        match classify_session_command("SET timezone = UTC") {
            Some(QueryClass::Set { value, .. }) => {
                assert_eq!(value, "UTC");
            }
            other => panic!("Expected Set, got {:?}", other),
        }
    }

    #[test]
    fn test_show_transaction_isolation_level() {
        match classify_session_command("SHOW TRANSACTION ISOLATION LEVEL") {
            Some(QueryClass::Show { key }) => {
                assert_eq!(
                    key, "transaction_isolation",
                    "SHOW TRANSACTION ISOLATION LEVEL must map to the 'transaction_isolation' metadata key"
                );
            }
            other => panic!("Expected Show, got {:?}", other),
        }
    }

    #[test]
    fn test_show_transaction_isolation_level_with_semicolon() {
        match classify_session_command("SHOW TRANSACTION ISOLATION LEVEL;") {
            Some(QueryClass::Show { key }) => {
                assert_eq!(key, "transaction_isolation");
            }
            other => panic!("Expected Show, got {:?}", other),
        }
    }
}
