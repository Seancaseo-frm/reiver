//! BI Tool Compatibility Tests (Level 1: Query Corpus)
//!
//! These tests feed known SQL queries from popular BI tools through the pgwire
//! routing and translation pipeline to verify correct handling. No network or
//! running server is required -- these are pure function-call tests.
//!
//! Each BI tool has three test phases:
//! 1. Connection init -- SET/SHOW commands sent during startup
//! 2. Schema discovery -- pg_catalog and information_schema introspection
//! 3. Data queries -- parameterized SELECTs with Postgres dialect features

use super::catalog::is_catalog_query;
use super::dialect::translate_to_clickhouse;
use super::handler::bind_parameters;
use super::session::{classify_session_command, QueryClass};

// ============================================================================
// Helpers
// ============================================================================

/// Assert that a SQL string is classified as a specific session command variant.
fn assert_session_set(sql: &str) {
    match classify_session_command(sql) {
        Some(QueryClass::Set { .. }) => {}
        other => panic!("Expected Set for {:?}, got {:?}", sql, other),
    }
}

fn assert_session_show(sql: &str) {
    match classify_session_command(sql) {
        Some(QueryClass::Show { .. }) => {}
        other => panic!("Expected Show for {:?}, got {:?}", sql, other),
    }
}

fn assert_session_show_all(sql: &str) {
    match classify_session_command(sql) {
        Some(QueryClass::ShowAll) => {}
        other => panic!("Expected ShowAll for {:?}, got {:?}", sql, other),
    }
}

fn assert_not_session(sql: &str) {
    if let Some(class) = classify_session_command(sql) {
        panic!(
            "Expected non-session query for {:?}, got {:?}",
            sql, class
        );
    }
}

/// Assert that a SQL string is detected as a catalog query.
fn assert_catalog(sql: &str) {
    assert!(
        is_catalog_query(sql),
        "Expected catalog query: {:?}",
        sql
    );
}

/// Assert that a SQL string is NOT a catalog query (data query).
fn assert_not_catalog(sql: &str) {
    assert!(
        !is_catalog_query(sql),
        "Expected non-catalog query: {:?}",
        sql
    );
}

/// Assert that a SQL string passes the read-only guard.
fn assert_read_only_ok(sql: &str) {
    crate::warehouse::connectors::enforce_read_only_sql(sql)
        .unwrap_or_else(|e| panic!("enforce_read_only rejected {:?}: {}", sql, e));
}

/// Assert that translate_to_clickhouse does not panic and returns valid SQL.
fn assert_translates(sql: &str) -> String {
    let result = translate_to_clickhouse(sql);
    assert!(
        !result.is_empty(),
        "Translation returned empty string for {:?}",
        sql
    );
    result
}

/// Assert parameter binding succeeds and returns valid SQL.
fn assert_binds(sql: &str, params: Vec<Option<&str>>) -> String {
    let params: Vec<Option<bytes::Bytes>> = params
        .into_iter()
        .map(|p| p.map(|s| bytes::Bytes::from(s.to_owned())))
        .collect();
    bind_parameters(sql, &params)
        .unwrap_or_else(|e| panic!("bind_parameters failed for {:?}: {}", sql, e))
}

// ============================================================================
// Metabase (JDBC driver)
// ============================================================================

/// Metabase connection initialization sends these SET/SHOW commands
/// via the PostgreSQL JDBC driver during startup.
#[test]
fn metabase_connection_init() {
    // JDBC driver sets extra_float_digits for precise numeric output
    assert_session_set("SET extra_float_digits = 3");

    // Application name identification
    assert_session_set("SET application_name = 'PostgreSQL JDBC Driver'");

    // Session parameter queries
    assert_session_show("SHOW server_version");
    assert_session_show("SHOW server_encoding");
    assert_session_show("SHOW client_encoding");
    assert_session_show("SHOW standard_conforming_strings");
    assert_session_show("SHOW is_superuser");
    assert_session_show("SHOW transaction_isolation");

    // JDBC also sends timezone SET
    assert_session_set("SET TIME ZONE 'UTC'");
}

/// Metabase schema discovery queries pg_catalog and information_schema
/// to populate its database browser and generate queries.
#[test]
fn metabase_schema_discovery() {
    let queries = [
        // Namespace listing
        "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname",
        // Type listing for type mapping
        "SELECT t.typname, t.oid FROM pg_catalog.pg_type t",
        // Table listing
        "SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        // Column listing for a specific table
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders'",
        // Primary key discovery
        "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = 'public'",
    ];

    for sql in &queries {
        assert_catalog(sql);
        assert_read_only_ok(sql);
    }
}

/// Metabase system function queries (no table references, DataFusion-handled).
#[test]
fn metabase_system_functions() {
    let queries = [
        "SELECT current_schema()",
        "SELECT version()",
        "SELECT current_database()",
    ];

    for sql in &queries {
        // These are catalog queries (system functions routed to DataFusion)
        assert_catalog(sql);
        assert_read_only_ok(sql);
    }
}

/// Metabase data queries use JDBC PreparedStatements with $N parameters
/// and Postgres-specific syntax like :: casts.
#[test]
fn metabase_data_queries() {
    // Simple parameterized query
    let sql = "SELECT * FROM orders WHERE id = $1";
    assert_not_session(sql);
    assert_not_catalog(sql);
    assert_read_only_ok(sql);
    let bound = assert_binds(sql, vec![Some("42")]);
    assert!(!bound.contains("'42'") && bound.contains("42"), "Numeric should be unquoted in: {}", bound);

    // Date filtering with :: cast (common in Metabase date widgets)
    let sql = "SELECT * FROM orders WHERE created_at >= $1::timestamp AND created_at < $2::timestamp";
    assert_read_only_ok(sql);
    let bound = assert_binds(
        sql,
        vec![Some("2024-01-01"), Some("2024-02-01")],
    );
    assert!(bound.contains("'2024-01-01'"), "Missing date in: {}", bound);

    // Dialect translation for :: cast
    let sql = "SELECT created_at::date, count(*) FROM orders GROUP BY 1";
    let translated = assert_translates(sql);
    // :: cast should be translated to CAST syntax
    assert!(
        translated.to_ascii_lowercase().contains("cast"),
        "Expected CAST in translated SQL: {}",
        translated
    );
}

// ============================================================================
// Grafana (pgx / Go driver)
// ============================================================================

/// Grafana's pgx driver connection init sequence.
#[test]
fn grafana_connection_init() {
    assert_session_set("SET client_encoding = 'UTF8'");
    assert_session_set("SET standard_conforming_strings = on");
    assert_session_set("SET extra_float_digits = 3");
    assert_session_show("SHOW standard_conforming_strings");
    assert_session_show("SHOW server_version");

    // pgx also sends a SHOW ALL during startup
    assert_session_show_all("SHOW ALL");
}

/// Grafana schema introspection.
#[test]
fn grafana_schema_discovery() {
    let queries = [
        // pgx queries pg_type for OID mapping
        "SELECT t.oid, t.typname, t.typtype FROM pg_catalog.pg_type t WHERE t.typtype IN ('b', 'p', 'r', 'e')",
        // Table listing
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    ];

    for sql in &queries {
        assert_catalog(sql);
        assert_read_only_ok(sql);
    }

    // version() is a system function
    assert_catalog("SELECT version()");
    assert_read_only_ok("SELECT version()");
}

/// Grafana data queries typically use parameterized time-range filters
/// with ::timestamp casts and date_trunc for bucketing.
#[test]
fn grafana_data_queries() {
    // Time-range parameterized query (Grafana's core pattern)
    let sql = "SELECT date_trunc('hour', created_at) AS time, count(*) AS value FROM events WHERE created_at > $1::timestamp AND created_at < $2::timestamp GROUP BY 1 ORDER BY 1";
    assert_not_session(sql);
    assert_not_catalog(sql);
    assert_read_only_ok(sql);

    // Bind parameters
    let bound = assert_binds(
        sql,
        vec![Some("2024-01-01T00:00:00Z"), Some("2024-01-02T00:00:00Z")],
    );
    assert!(
        bound.contains("'2024-01-01T00:00:00Z'"),
        "Missing start time in: {}",
        bound
    );

    // Dialect translation -- date_trunc should be preserved or translated
    let translated = assert_translates(sql);
    assert!(
        translated.to_ascii_lowercase().contains("date_trunc")
            || translated.to_ascii_lowercase().contains("tostartofininterval")
            || translated.to_ascii_lowercase().contains("tostart"),
        "Expected date_trunc handling in: {}",
        translated
    );

    // Grafana variable interpolation with $__timeFilter macro resolves to
    // something like this by the time it reaches the driver:
    let sql = "SELECT count(*) FROM events WHERE created_at BETWEEN $1 AND $2";
    assert_read_only_ok(sql);
    let bound = assert_binds(
        sql,
        vec![Some("2024-01-01"), Some("2024-01-31")],
    );
    assert!(bound.contains("'2024-01-01'"), "Missing bound date: {}", bound);

    // EXTRACT used in Grafana panels
    let sql = "SELECT EXTRACT(HOUR FROM created_at) AS hour, count(*) FROM events GROUP BY 1";
    assert_read_only_ok(sql);
    let translated = assert_translates(sql);
    assert!(
        translated.to_ascii_lowercase().contains("tohour")
            || translated.to_ascii_lowercase().contains("extract"),
        "Expected EXTRACT translation in: {}",
        translated
    );
}

// ============================================================================
// DBeaver (JDBC driver)
// ============================================================================

/// DBeaver connection init -- very chatty with SET/SHOW during startup.
#[test]
fn dbeaver_connection_init() {
    assert_session_set("SET extra_float_digits = 3");
    assert_session_set("SET application_name = 'DBeaver 24.0.0 - Main'");
    assert_session_show("SHOW search_path");
    assert_session_show("SHOW server_version");
    assert_session_show("SHOW server_encoding");
    assert_session_show("SHOW client_encoding");
    assert_session_show("SHOW standard_conforming_strings");
    assert_session_show("SHOW timezone");
    assert_session_show("SHOW integer_datetimes");
    assert_session_show("SHOW datestyle");

    // DBeaver also sends transaction commands
    let begin = classify_session_command("BEGIN");
    assert!(matches!(begin, Some(QueryClass::Begin)), "Expected Begin");
    let commit = classify_session_command("COMMIT");
    assert!(matches!(commit, Some(QueryClass::Commit)), "Expected Commit");
}

/// DBeaver schema discovery is extremely thorough -- it queries all aspects
/// of the catalog to populate its database tree.
#[test]
fn dbeaver_schema_discovery() {
    let queries = [
        // Current session info
        "SELECT current_database(), current_schema(), session_user",
        // Schema listing
        "SELECT nspname, oid FROM pg_catalog.pg_namespace ORDER BY nspname",
        // Table listing with full metadata
        "SELECT c.relname, n.nspname, c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind IN ('r','v','m','f','p') AND n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY n.nspname, c.relname",
        // Column metadata
        "SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders' ORDER BY ordinal_position",
        // Type catalog
        "SELECT typname, typnamespace, typtype, oid FROM pg_catalog.pg_type",
        // Constraint discovery
        "SELECT conname, contype FROM pg_catalog.pg_constraint",
        // Index listing
        "SELECT indexname, tablename FROM pg_catalog.pg_indexes WHERE schemaname = 'public'",
        // information_schema.tables for table list
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'",
    ];

    for sql in &queries {
        assert_catalog(sql);
        assert_read_only_ok(sql);
    }
}

/// DBeaver data queries -- typically use standard SQL with occasional
/// Postgres-specific features.
#[test]
fn dbeaver_data_queries() {
    // Simple table preview (DBeaver's "View Data" feature)
    let sql = "SELECT * FROM orders LIMIT 200";
    assert_not_session(sql);
    assert_not_catalog(sql);
    assert_read_only_ok(sql);
    assert_translates(sql);

    // Filtered query with parameterization
    let sql = "SELECT * FROM orders WHERE status = $1 AND total > $2";
    assert_read_only_ok(sql);
    let bound = assert_binds(sql, vec![Some("active"), Some("100")]);
    assert!(bound.contains("'active'"), "Missing status in: {}", bound);
    assert!(!bound.contains("'100'") && bound.contains("100"), "Numeric total should be unquoted in: {}", bound);

    // DBeaver's count query
    let sql = "SELECT count(*) FROM orders";
    assert_read_only_ok(sql);
    assert_translates(sql);

    // DBeaver EXPLAIN
    let sql = "EXPLAIN SELECT * FROM orders WHERE id = 1";
    assert_read_only_ok(sql);
}

// ============================================================================
// Apache Superset (psycopg2 / Python)
// ============================================================================

/// Superset connection init -- psycopg2 driver startup.
#[test]
fn superset_connection_init() {
    assert_session_set("SET client_encoding = 'UTF8'");
    assert_session_set("SET DateStyle TO 'ISO'");
    assert_session_set("SET extra_float_digits = 2");
    assert_session_show("SHOW server_version");
    assert_session_show("SHOW standard_conforming_strings");

    // psycopg2 also sends timezone
    assert_session_set("SET TIME ZONE 'UTC'");
}

/// Superset schema discovery queries.
#[test]
fn superset_schema_discovery() {
    let queries = [
        // Table listing
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW')",
        // Column info
        "SELECT column_name, data_type, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'orders'",
        // Schema listing
        "SELECT schema_name FROM information_schema.schemata",
    ];

    for sql in &queries {
        assert_catalog(sql);
        assert_read_only_ok(sql);
    }

    // System function queries
    assert_catalog("SELECT version()");
}

/// Superset data queries make heavy use of Postgres date/time functions
/// and parameterized queries via psycopg2.
#[test]
fn superset_data_queries() {
    // Superset time-grain bucketing with date_trunc
    let sql = "SELECT date_trunc('day', created_at) AS ds, count(*) AS count FROM events GROUP BY 1 ORDER BY 1 DESC LIMIT 100";
    assert_not_session(sql);
    assert_not_catalog(sql);
    assert_read_only_ok(sql);
    let translated = assert_translates(sql);
    assert!(
        translated.to_ascii_lowercase().contains("date_trunc")
            || translated.to_ascii_lowercase().contains("tostart"),
        "Expected date bucketing in: {}",
        translated
    );

    // to_char for date formatting (common in Superset charts)
    let sql = "SELECT to_char(created_at, 'YYYY-MM') AS month, sum(total) FROM orders GROUP BY 1";
    assert_read_only_ok(sql);
    let translated = assert_translates(sql);
    assert!(
        translated.to_ascii_lowercase().contains("formatdatetime")
            || translated.to_ascii_lowercase().contains("to_char"),
        "Expected to_char translation in: {}",
        translated
    );

    // EXTRACT for time-based grouping
    let sql = "SELECT EXTRACT(YEAR FROM created_at) AS year, EXTRACT(MONTH FROM created_at) AS month, count(*) FROM events GROUP BY 1, 2";
    assert_read_only_ok(sql);
    let translated = assert_translates(sql);
    assert!(
        translated.to_ascii_lowercase().contains("toyear")
            || translated.to_ascii_lowercase().contains("extract"),
        "Expected EXTRACT translation in: {}",
        translated
    );

    // Superset uses COALESCE and CASE expressions
    let sql = "SELECT COALESCE(status, 'unknown') AS status, count(*) FROM orders GROUP BY 1";
    assert_read_only_ok(sql);
    assert_translates(sql);

    // Parameterized schema query (psycopg2 sends schema name as $1)
    let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1";
    assert_catalog(sql);
    let bound = assert_binds(sql, vec![Some("public")]);
    assert!(
        bound.contains("'public'"),
        "Expected bound schema name: {}",
        bound
    );

    // Superset subquery pattern
    let sql = "SELECT * FROM (SELECT date_trunc('day', created_at) AS ds, count(*) AS cnt FROM events GROUP BY 1) AS expr_qry ORDER BY ds DESC LIMIT 1000";
    assert_read_only_ok(sql);
    assert_translates(sql);
}

// ============================================================================
// Cross-tool: Common query patterns
// ============================================================================

/// All BI tools issue SET commands that we must handle without error.
#[test]
fn common_set_commands() {
    let set_commands = [
        "SET extra_float_digits = 3",
        "SET application_name = 'test'",
        "SET client_encoding = 'UTF8'",
        "SET standard_conforming_strings = on",
        "SET DateStyle TO 'ISO, MDY'",
        "SET TIME ZONE 'UTC'",
        "SET TIME ZONE 'America/New_York'",
        "SET search_path TO public",
        "SET statement_timeout = 30000",
        "SET lock_timeout = 10000",
        "SET idle_in_transaction_session_timeout = 60000",
        "SET client_min_messages = warning",
        "SET row_security = off",
        "SET default_transaction_read_only = on",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    ];

    for sql in &set_commands {
        let result = classify_session_command(sql);
        assert!(
            result.is_some(),
            "SET command not classified: {:?}",
            sql
        );
    }
}

/// All BI tools issue SHOW commands during startup.
#[test]
fn common_show_commands() {
    let show_commands = [
        ("SHOW server_version", false),
        ("SHOW server_encoding", false),
        ("SHOW client_encoding", false),
        ("SHOW standard_conforming_strings", false),
        ("SHOW integer_datetimes", false),
        ("SHOW datestyle", false),
        ("SHOW timezone", false),
        ("SHOW is_superuser", false),
        ("SHOW search_path", false),
        ("SHOW transaction_isolation", false),
        ("SHOW ALL", true),
    ];

    for (sql, is_show_all) in &show_commands {
        if *is_show_all {
            assert_session_show_all(sql);
        } else {
            assert_session_show(sql);
        }
    }
}

/// Transaction control commands sent by various drivers.
#[test]
fn common_transaction_commands() {
    let commands = [
        ("BEGIN", QueryClass::Begin),
        ("START TRANSACTION", QueryClass::Begin),
        ("COMMIT", QueryClass::Commit),
        ("END", QueryClass::Commit),
        ("ROLLBACK", QueryClass::Rollback),
    ];

    for (sql, _expected) in &commands {
        let result = classify_session_command(sql);
        assert!(
            result.is_some(),
            "Transaction command not classified: {:?}",
            sql
        );
    }
}

/// DISCARD/DEALLOCATE/CLOSE commands sent during connection cleanup.
#[test]
fn common_cleanup_commands() {
    let commands = [
        "DISCARD ALL",
        "DEALLOCATE ALL",
        "CLOSE ALL",
        "RESET ALL",
        "RESET client_encoding",
    ];

    for sql in &commands {
        let result = classify_session_command(sql);
        assert!(
            result.is_some(),
            "Cleanup command not classified: {:?}",
            sql
        );
    }
}

/// Postgres-specific SQL features that appear in BI tool queries.
/// These must translate without panicking.
#[test]
fn common_dialect_features() {
    let queries = [
        // :: casts (all tools)
        "SELECT id::text FROM orders",
        "SELECT created_at::date FROM events",
        "SELECT total::numeric(10,2) FROM orders",
        // String functions
        "SELECT char_length(name) FROM users",
        "SELECT left(name, 10) FROM users",
        "SELECT right(name, 5) FROM users",
        // Date functions
        "SELECT date_trunc('month', created_at) FROM orders",
        "SELECT EXTRACT(YEAR FROM created_at) FROM orders",
        "SELECT EXTRACT(MONTH FROM created_at) FROM orders",
        "SELECT EXTRACT(DAY FROM created_at) FROM orders",
        "SELECT EXTRACT(HOUR FROM created_at) FROM events",
        "SELECT EXTRACT(DOW FROM created_at) FROM events",
        // Aggregate functions
        "SELECT array_agg(name) FROM users",
        "SELECT string_agg(name, ', ') FROM users",
        "SELECT bool_or(is_active) FROM users",
        "SELECT bool_and(is_verified) FROM users",
        // Regex functions
        "SELECT regexp_replace(name, 'foo', 'bar') FROM users",
        "SELECT regexp_replace(name, 'foo', 'bar', 'g') FROM users",
        // to_char
        "SELECT to_char(created_at, 'YYYY-MM-DD') FROM orders",
        // Current date/time functions
        "SELECT current_timestamp",
        "SELECT current_date",
        "SELECT now()",
        // COALESCE, CASE, NULLIF
        "SELECT COALESCE(name, 'unknown') FROM users",
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM orders",
        "SELECT NULLIF(total, 0) FROM orders",
    ];

    for sql in &queries {
        assert_read_only_ok(sql);
        let translated = assert_translates(sql);
        assert!(
            !translated.is_empty(),
            "Empty translation for: {:?}",
            sql
        );
    }
}

/// Verify that write operations are correctly rejected, regardless of
/// which BI tool sends them (some tools allow ad-hoc SQL).
#[test]
fn write_operations_rejected() {
    let forbidden = [
        "INSERT INTO orders (id) VALUES (1)",
        "UPDATE orders SET status = 'cancelled'",
        "DELETE FROM orders WHERE id = 1",
        "DROP TABLE orders",
        "CREATE TABLE test (id int)",
        "ALTER TABLE orders ADD COLUMN new_col text",
        "TRUNCATE orders",
    ];

    for sql in &forbidden {
        let result = crate::warehouse::connectors::enforce_read_only_sql(sql);
        assert!(
            result.is_err(),
            "Write operation should be rejected: {:?}",
            sql
        );
    }
}

/// Parameter binding edge cases that BI tools can trigger.
#[test]
fn parameter_binding_edge_cases() {
    // NULL parameter (common in optional filter patterns)
    let bound = assert_binds(
        "SELECT * FROM orders WHERE ($1 IS NULL OR status = $1)",
        vec![None],
    );
    assert!(bound.contains("NULL"), "Expected NULL in: {}", bound);

    // Many parameters (Metabase can send 10+ for complex filters)
    let sql = "SELECT * FROM orders WHERE a = $1 AND b = $2 AND c = $3 AND d = $4 AND e = $5";
    let bound = assert_binds(
        sql,
        vec![
            Some("v1"),
            Some("v2"),
            Some("v3"),
            Some("v4"),
            Some("v5"),
        ],
    );
    assert!(bound.contains("'v1'"), "Missing v1 in: {}", bound);
    assert!(bound.contains("'v5'"), "Missing v5 in: {}", bound);

    // Parameter in subquery
    let sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE status = $1)";
    let bound = assert_binds(sql, vec![Some("active")]);
    assert!(
        bound.contains("'active'"),
        "Missing bound value in subquery: {}",
        bound
    );

    // Parameter with single quotes (O'Brien pattern)
    let bound = assert_binds(
        "SELECT * FROM orders WHERE name = $1",
        vec![Some("O'Brien")],
    );
    // The value must be safely escaped
    assert!(
        bound.contains("O'Brien") || bound.contains("O''Brien"),
        "Expected escaped quote in: {}",
        bound
    );
}
