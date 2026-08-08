//! Per-variable definition for `llm_prompt_versions.variables` (JSON array).
//!
//! Single source of truth in the Flow crate: used at create-version validation and
//! at gateway `validate_prompt_variables`. Wire JSON key is `type` (alias `var_type`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One slot in the prompt version `variables` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableDefinition {
    /// Variable name (used in templates as `{{name}}`)
    pub name: String,
    /// Optional description for documentation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `"string"` | `"number"` | `"boolean"` | `"json"` | `"enum"`
    #[serde(rename = "type", alias = "var_type", default = "default_var_type")]
    pub var_type: String,
    /// Whether a value must be supplied (missing + no default → error)
    #[serde(default)]
    pub required: bool,
    /// Value injected when the variable is absent and not required
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Allowed values for `var_type == "enum"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
    /// Max UTF-8 character count for `var_type == "string"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    /// Inclusive lower bound for `var_type == "number"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Inclusive upper bound for `var_type == "number"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

fn default_var_type() -> String {
    "string".to_string()
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

impl VariableDefinition {
    /// Validate that the variable name is valid for Handlebars templates.
    pub fn validate_name(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Variable name cannot be empty".to_string());
        }

        if self
            .name
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        {
            return Err(format!(
                "Variable name '{}' cannot start with a digit",
                self.name
            ));
        }

        if !self.name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!(
                "Variable name '{}' contains invalid characters (only alphanumeric and underscore allowed)",
                self.name
            ));
        }

        Ok(())
    }

    /// Validate that a value matches the expected [`Self::var_type`].
    pub fn validate_value(&self, value: &Value) -> Result<(), String> {
        match self.var_type.as_str() {
            "string" => {
                if !value.is_string() && !value.is_null() {
                    return Err(format!(
                        "Variable '{}' expected string, got {}",
                        self.name,
                        value_type_name(value)
                    ));
                }
            }
            "number" => {
                if !value.is_number() && !value.is_null() {
                    return Err(format!(
                        "Variable '{}' expected number, got {}",
                        self.name,
                        value_type_name(value)
                    ));
                }
            }
            "boolean" => {
                if !value.is_boolean() && !value.is_null() {
                    return Err(format!(
                        "Variable '{}' expected boolean, got {}",
                        self.name,
                        value_type_name(value)
                    ));
                }
            }
            "json" => {
                // Any JSON value is valid
            }
            _ => {
                // Unknown type - treat as any
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_type_key() {
        let v: Vec<VariableDefinition> = serde_json::from_value(serde_json::json!([{
            "name": "x", "type": "string", "required": true
        }]))
        .unwrap();
        assert_eq!(v[0].name, "x");
        assert_eq!(v[0].var_type, "string");
    }

    #[test]
    fn deserializes_var_type_alias() {
        let v: Vec<VariableDefinition> = serde_json::from_value(serde_json::json!([{
            "name": "x", "var_type": "number", "required": false
        }]))
        .unwrap();
        assert_eq!(v[0].var_type, "number");
    }

    #[test]
    fn serializes_type_not_var_type() {
        let v = VariableDefinition {
            name: "a".to_string(),
            description: None,
            var_type: "string".to_string(),
            required: true,
            default: None,
            values: None,
            max_chars: None,
            min: None,
            max: None,
        };
        let json = serde_json::to_value(&v).unwrap();
        assert!(json.get("type").is_some());
        assert!(json.get("var_type").is_none());
    }

    #[test]
    fn validate_name_valid() {
        for name in ["name", "user_name", "userName", "name123", "_private"] {
            let def = VariableDefinition {
                name: name.to_string(),
                description: None,
                var_type: "string".to_string(),
                required: false,
                default: None,
                values: None,
                max_chars: None,
                min: None,
                max: None,
            };
            assert!(
                def.validate_name().is_ok(),
                "Name '{}' should be valid",
                name
            );
        }
    }

    #[test]
    fn validate_name_invalid() {
        for (name, reason) in [
            ("", "empty"),
            ("123name", "starts with digit"),
            ("user-name", "hyphen"),
        ] {
            let def = VariableDefinition {
                name: name.to_string(),
                description: None,
                var_type: "string".to_string(),
                required: false,
                default: None,
                values: None,
                max_chars: None,
                min: None,
                max: None,
            };
            assert!(
                def.validate_name().is_err(),
                "Name '{}' should be invalid ({})",
                name,
                reason
            );
        }
    }
}
