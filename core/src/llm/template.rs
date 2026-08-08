//! Prompt Template Engine
//!
//! Provides Handlebars-based template compilation for prompt variables.
//! Supports type validation, default values, and error handling.
//!
//! # Performance
//!
//! Uses a static Handlebars instance to avoid allocation overhead on each
//! template compilation. The Handlebars registry is thread-safe and can be
//! shared across all requests.
//!
//! # Built-in Helpers
//!
//! The following helpers are available in all templates:
//! - String: `uppercase`, `lowercase`, `capitalize`, `truncate`, `trim`, `replace`
//! - Number: `formatNumber`, `round`, `abs`
//! - Date: `today`, `now`, `formatDate`, `relativeTime`
//! - Logic: `default`, `coalesce`, `eq`, `ne`, `gt`, `lt`, `gte`, `lte`
//! - JSON: `json`, `jsonPretty`

use handlebars::Handlebars;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;

use crate::error::AppError;
use crate::llm::helpers::register_builtin_helpers;

/// Static Handlebars instance shared across all template compilations.
/// This avoids the overhead of creating a new registry for each request.
/// Strict mode is enabled to catch missing variables early.
/// Built-in helpers are registered for common operations.
static HANDLEBARS: Lazy<Handlebars<'static>> = Lazy::new(|| {
    let mut hb = Handlebars::new();
    hb.set_strict_mode(true); // Error on missing variables instead of silent empty string
    register_builtin_helpers(&mut hb); // Register all built-in helpers
    hb
});

/// Compile a template with the given variables.
///
/// # Arguments
/// * `template` - Handlebars template string (e.g., "Hello {{user_name}}")
/// * `variables` - HashMap of variable name to value
///
/// # Returns
/// The compiled string with variables substituted
///
/// # Performance
/// Uses a shared static Handlebars instance to avoid allocation overhead.
pub fn compile_prompt(
    template: &str,
    variables: &HashMap<String, Value>,
) -> Result<String, AppError> {
    HANDLEBARS
        .render_template(template, variables)
        .map_err(|e| AppError::Validation(format!("Template compilation error: {}", e)))
}

/// Extract variable names from a template.
///
/// Returns a list of variable names used in the template.
/// This is useful for validation and documentation.
pub fn extract_variable_names(template: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut chars = template.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '{' && chars.peek() == Some(&'{') {
            chars.next(); // consume second '{'

            // Skip whitespace
            while chars.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
                chars.next();
            }

            // Collect variable name
            let mut name = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' {
                    name.push(c);
                    chars.next();
                } else {
                    break;
                }
            }

            if !name.is_empty() && !names.contains(&name) {
                names.push(name);
            }
        }
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_prompt_basic() {
        let mut vars = HashMap::new();
        vars.insert("name".to_string(), Value::String("World".to_string()));

        let result = compile_prompt("Hello, {{name}}!", &vars).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_compile_prompt_multiple_vars() {
        let mut vars = HashMap::new();
        vars.insert("name".to_string(), Value::String("Alice".to_string()));
        vars.insert("role".to_string(), Value::String("developer".to_string()));

        let result = compile_prompt("You are {{name}}, a {{role}}.", &vars).unwrap();
        assert_eq!(result, "You are Alice, a developer.");
    }

    #[test]
    fn test_extract_variable_names() {
        let names = extract_variable_names("Hello {{name}}, you have {{count}} messages.");
        assert_eq!(names, vec!["name", "count"]);
    }

    #[test]
    fn test_extract_variable_names_dedup() {
        let names = extract_variable_names("{{name}} and {{name}} again");
        assert_eq!(names, vec!["name"]);
    }
}
