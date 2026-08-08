//! Parse-once wrapper for SQL statements.
//!
//! `ParsedQuery` holds the parsed AST so that downstream pipeline stages
//! (router, rewriter, cost estimator, cache) can operate on the AST directly
//! instead of re-parsing the same SQL string at every stage.

use sqlparser::ast::Statement;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

use super::rewriter::serialize_statements;

/// A SQL query that has been parsed exactly once.
///
/// Wraps the sqlparser AST and provides cheap access to both the statements
/// and a serialized SQL string.  All pipeline stages should accept
/// `&ParsedQuery` (or `&[Statement]` via [`Self::statements`]) instead of
/// raw `&str` whenever possible.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    statements: Vec<Statement>,
}

impl ParsedQuery {
    /// Parse a SQL string using the ClickHouse dialect.
    pub fn parse(sql: &str) -> Result<Self, sqlparser::parser::ParserError> {
        let dialect = ClickHouseDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        Ok(Self { statements })
    }

    /// Wrap pre-parsed statements (zero-cost).
    pub fn from_statements(statements: Vec<Statement>) -> Self {
        Self { statements }
    }

    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }

    pub fn statements_mut(&mut self) -> &mut Vec<Statement> {
        &mut self.statements
    }

    /// Serialize the AST back to a SQL string.
    pub fn to_sql(&self) -> String {
        serialize_statements(&self.statements)
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}
