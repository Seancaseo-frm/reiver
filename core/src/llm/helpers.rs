//! Built-in Handlebars Helpers
//!
//! Provides a set of built-in template helpers for prompt templates.
//! These helpers are implemented in Rust for safety and performance.

use handlebars::{
    Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext, RenderError,
    RenderErrorReason, ScopedJson,
};
use serde_json::Value;

/// Create a RenderError from a message string.
fn render_error(msg: impl Into<String>) -> RenderError {
    RenderErrorReason::Other(msg.into()).into()
}

/// Register all built-in helpers with a Handlebars instance.
pub fn register_builtin_helpers(handlebars: &mut Handlebars) {
    // String helpers
    handlebars.register_helper("uppercase", Box::new(UppercaseHelper));
    handlebars.register_helper("lowercase", Box::new(LowercaseHelper));
    handlebars.register_helper("capitalize", Box::new(CapitalizeHelper));
    handlebars.register_helper("truncate", Box::new(TruncateHelper));
    handlebars.register_helper("trim", Box::new(TrimHelper));
    handlebars.register_helper("replace", Box::new(ReplaceHelper));

    // Number helpers
    handlebars.register_helper("formatNumber", Box::new(FormatNumberHelper));
    handlebars.register_helper("round", Box::new(RoundHelper));
    handlebars.register_helper("abs", Box::new(AbsHelper));

    // Date helpers
    handlebars.register_helper("today", Box::new(TodayHelper));
    handlebars.register_helper("now", Box::new(NowHelper));
    handlebars.register_helper("formatDate", Box::new(FormatDateHelper));
    handlebars.register_helper("relativeTime", Box::new(RelativeTimeHelper));

    // Logic helpers
    handlebars.register_helper("default", Box::new(DefaultHelper));
    handlebars.register_helper("coalesce", Box::new(CoalesceHelper));
    handlebars.register_helper("eq", Box::new(EqHelper));
    handlebars.register_helper("ne", Box::new(NeHelper));
    handlebars.register_helper("gt", Box::new(GtHelper));
    handlebars.register_helper("lt", Box::new(LtHelper));
    handlebars.register_helper("gte", Box::new(GteHelper));
    handlebars.register_helper("lte", Box::new(LteHelper));

    // JSON helpers
    handlebars.register_helper("json", Box::new(JsonHelper));
    handlebars.register_helper("jsonPretty", Box::new(JsonPrettyHelper));
}

// =============================================================================
// String Helpers
// =============================================================================

/// Convert string to uppercase: {{uppercase name}}
struct UppercaseHelper;

impl HelperDef for UppercaseHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("uppercase requires a string parameter"))?;

        out.write(&param.to_uppercase())?;
        Ok(())
    }
}

/// Convert string to lowercase: {{lowercase name}}
struct LowercaseHelper;

impl HelperDef for LowercaseHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("lowercase requires a string parameter"))?;

        out.write(&param.to_lowercase())?;
        Ok(())
    }
}

/// Capitalize first letter: {{capitalize name}}
struct CapitalizeHelper;

impl HelperDef for CapitalizeHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("capitalize requires a string parameter"))?;

        let mut chars = param.chars();
        if let Some(first) = chars.next() {
            out.write(&first.to_uppercase().to_string())?;
            out.write(chars.as_str())?;
        }
        Ok(())
    }
}

/// Truncate string to N characters with ellipsis: {{truncate text 100}}
struct TruncateHelper;

impl HelperDef for TruncateHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let text = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("truncate requires a string parameter"))?;

        let length = h.param(1).and_then(|v| v.value().as_u64()).unwrap_or(100) as usize;

        if text.chars().count() <= length {
            out.write(text)?;
        } else {
            // Truncate at character boundary
            let truncated: String = text.chars().take(length).collect();
            out.write(&truncated)?;
            out.write("...")?;
        }
        Ok(())
    }
}

/// Trim whitespace: {{trim text}}
struct TrimHelper;

impl HelperDef for TrimHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("trim requires a string parameter"))?;

        out.write(param.trim())?;
        Ok(())
    }
}

/// Replace substring: {{replace text "old" "new"}}
struct ReplaceHelper;

impl HelperDef for ReplaceHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let text = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("replace requires a string as first parameter"))?;

        let old = h
            .param(1)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("replace requires a pattern as second parameter"))?;

        let new = h
            .param(2)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("replace requires a replacement as third parameter"))?;

        out.write(&text.replace(old, new))?;
        Ok(())
    }
}

// =============================================================================
// Number Helpers
// =============================================================================

/// Format number with decimal places: {{formatNumber price 2}}
struct FormatNumberHelper;

impl HelperDef for FormatNumberHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h.param(0).map(|v| v.value());
        let decimals = h.param(1).and_then(|v| v.value().as_u64()).unwrap_or(2) as usize;

        let num = match value {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Some(Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            _ => return Err(render_error("formatNumber requires a number parameter")),
        };

        out.write(&format!("{:.prec$}", num, prec = decimals))?;
        Ok(())
    }
}

/// Round to nearest integer: {{round value}}
struct RoundHelper;

impl HelperDef for RoundHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h.param(0).map(|v| v.value());

        let num = match value {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Some(Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            _ => return Err(render_error("round requires a number parameter")),
        };

        out.write(&format!("{}", num.round() as i64))?;
        Ok(())
    }
}

/// Absolute value: {{abs value}}
struct AbsHelper;

impl HelperDef for AbsHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h.param(0).map(|v| v.value());

        let num = match value {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Some(Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            _ => return Err(render_error("abs requires a number parameter")),
        };

        // Check if it's an integer
        if num.fract() == 0.0 {
            out.write(&format!("{}", num.abs() as i64))?;
        } else {
            out.write(&format!("{}", num.abs()))?;
        }
        Ok(())
    }
}

// =============================================================================
// Date Helpers
// =============================================================================

/// Current UTC date as YYYY-MM-DD: {{today}}
struct TodayHelper;

impl HelperDef for TodayHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        _h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        out.write(&chrono::Utc::now().format("%Y-%m-%d").to_string())?;
        Ok(())
    }
}

/// Current UTC datetime as ISO 8601: {{now}}
struct NowHelper;

impl HelperDef for NowHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        _h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        out.write(&chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true))?;
        Ok(())
    }
}

/// Format date/timestamp: {{formatDate date "YYYY-MM-DD"}}
struct FormatDateHelper;

impl HelperDef for FormatDateHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let date_str = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("formatDate requires a date string parameter"))?;

        let format = h
            .param(1)
            .and_then(|v| v.value().as_str())
            .unwrap_or("YYYY-MM-DD");

        // Try to parse as ISO 8601 date
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
            // Convert format string from JavaScript style to chrono style
            // Order matters: replace longer patterns first to avoid partial matches
            let chrono_format = format
                .replace("YYYY", "%Y")
                .replace("MMM", "%b") // Before MM to avoid partial replacement
                .replace("MM", "%m")
                .replace("DD", "%d")
                .replace("D", "%-d") // After DD to avoid partial replacement
                .replace("HH", "%H")
                .replace("mm", "%M")
                .replace("ss", "%S");

            out.write(&dt.format(&chrono_format).to_string())?;
        } else if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
            // Same ordering for consistency
            let chrono_format = format
                .replace("YYYY", "%Y")
                .replace("MMM", "%b")
                .replace("MM", "%m")
                .replace("DD", "%d")
                .replace("D", "%-d");

            out.write(&dt.format(&chrono_format).to_string())?;
        } else {
            // Fallback: just output the original string
            out.write(date_str)?;
        }
        Ok(())
    }
}

/// Relative time: {{relativeTime date}}
struct RelativeTimeHelper;

impl HelperDef for RelativeTimeHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let date_str = h
            .param(0)
            .and_then(|v| v.value().as_str())
            .ok_or_else(|| render_error("relativeTime requires a date string parameter"))?;

        // Try to parse as ISO 8601 date
        let dt = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
            dt.with_timezone(&chrono::Utc)
        } else {
            // Fallback: just output the original string
            out.write(date_str)?;
            return Ok(());
        };

        let now = chrono::Utc::now();
        let duration = now.signed_duration_since(dt);

        let result = if duration.num_seconds().abs() < 60 {
            if duration.num_seconds() >= 0 {
                "just now".to_string()
            } else {
                "in a moment".to_string()
            }
        } else if duration.num_minutes().abs() < 60 {
            let mins = duration.num_minutes().abs();
            if duration.num_minutes() >= 0 {
                format!("{} minute{} ago", mins, if mins == 1 { "" } else { "s" })
            } else {
                format!("in {} minute{}", mins, if mins == 1 { "" } else { "s" })
            }
        } else if duration.num_hours().abs() < 24 {
            let hours = duration.num_hours().abs();
            if duration.num_hours() >= 0 {
                format!("{} hour{} ago", hours, if hours == 1 { "" } else { "s" })
            } else {
                format!("in {} hour{}", hours, if hours == 1 { "" } else { "s" })
            }
        } else if duration.num_days().abs() < 30 {
            let days = duration.num_days().abs();
            if duration.num_days() >= 0 {
                format!("{} day{} ago", days, if days == 1 { "" } else { "s" })
            } else {
                format!("in {} day{}", days, if days == 1 { "" } else { "s" })
            }
        } else if duration.num_days().abs() < 365 {
            let months = (duration.num_days().abs() / 30) as i64;
            if duration.num_days() >= 0 {
                format!("{} month{} ago", months, if months == 1 { "" } else { "s" })
            } else {
                format!("in {} month{}", months, if months == 1 { "" } else { "s" })
            }
        } else {
            let years = (duration.num_days().abs() / 365) as i64;
            if duration.num_days() >= 0 {
                format!("{} year{} ago", years, if years == 1 { "" } else { "s" })
            } else {
                format!("in {} year{}", years, if years == 1 { "" } else { "s" })
            }
        };

        out.write(&result)?;
        Ok(())
    }
}

// =============================================================================
// Logic Helpers
// =============================================================================

/// Default value if empty: {{default value "fallback"}}
struct DefaultHelper;

impl HelperDef for DefaultHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h.param(0).map(|v| v.value());
        let fallback = h.param(1).map(|v| v.value());

        let use_fallback = match value {
            None => true,
            Some(Value::Null) => true,
            Some(Value::String(s)) if s.is_empty() => true,
            Some(Value::Array(a)) if a.is_empty() => true,
            Some(Value::Object(o)) if o.is_empty() => true,
            _ => false,
        };

        if use_fallback {
            if let Some(fb) = fallback {
                match fb {
                    Value::String(s) => out.write(s)?,
                    Value::Number(n) => out.write(&n.to_string())?,
                    Value::Bool(b) => out.write(&b.to_string())?,
                    _ => out.write(&fb.to_string())?,
                }
            }
        } else if let Some(v) = value {
            match v {
                Value::String(s) => out.write(s)?,
                Value::Number(n) => out.write(&n.to_string())?,
                Value::Bool(b) => out.write(&b.to_string())?,
                _ => out.write(&v.to_string())?,
            }
        }
        Ok(())
    }
}

/// First non-null value: {{coalesce a b c}}
struct CoalesceHelper;

impl HelperDef for CoalesceHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        for param in h.params() {
            let value = param.value();
            if !value.is_null() {
                match value {
                    Value::String(s) if !s.is_empty() => {
                        out.write(s)?;
                        return Ok(());
                    }
                    Value::String(_) => continue, // empty string
                    Value::Number(n) => {
                        out.write(&n.to_string())?;
                        return Ok(());
                    }
                    Value::Bool(b) => {
                        out.write(&b.to_string())?;
                        return Ok(());
                    }
                    _ => {
                        out.write(&value.to_string())?;
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

/// Equality comparison for use in conditionals: {{#if (eq a b)}}
struct EqHelper;

impl HelperDef for EqHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h.param(0).map(|v| v.value());
        let b = h.param(1).map(|v| v.value());

        let result = match (a, b) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        };

        Ok(ScopedJson::Derived(Value::Bool(result)))
    }
}

/// Not equal comparison: {{#if (ne a b)}}
struct NeHelper;

impl HelperDef for NeHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h.param(0).map(|v| v.value());
        let b = h.param(1).map(|v| v.value());

        let result = match (a, b) {
            (Some(a), Some(b)) => a != b,
            (None, None) => false,
            _ => true,
        };

        Ok(ScopedJson::Derived(Value::Bool(result)))
    }
}

/// Greater than comparison: {{#if (gt a b)}}
struct GtHelper;

impl HelperDef for GtHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h
            .param(0)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("gt requires numeric parameters"))?;
        let b = h
            .param(1)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("gt requires numeric parameters"))?;

        Ok(ScopedJson::Derived(Value::Bool(a > b)))
    }
}

/// Less than comparison: {{#if (lt a b)}}
struct LtHelper;

impl HelperDef for LtHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h
            .param(0)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("lt requires numeric parameters"))?;
        let b = h
            .param(1)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("lt requires numeric parameters"))?;

        Ok(ScopedJson::Derived(Value::Bool(a < b)))
    }
}

/// Greater than or equal comparison: {{#if (gte a b)}}
struct GteHelper;

impl HelperDef for GteHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h
            .param(0)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("gte requires numeric parameters"))?;
        let b = h
            .param(1)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("gte requires numeric parameters"))?;

        Ok(ScopedJson::Derived(Value::Bool(a >= b)))
    }
}

/// Less than or equal comparison: {{#if (lte a b)}}
struct LteHelper;

impl HelperDef for LteHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let a = h
            .param(0)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("lte requires numeric parameters"))?;
        let b = h
            .param(1)
            .and_then(|v| v.value().as_f64())
            .ok_or_else(|| render_error("lte requires numeric parameters"))?;

        Ok(ScopedJson::Derived(Value::Bool(a <= b)))
    }
}

// =============================================================================
// JSON Helpers
// =============================================================================

/// Stringify to JSON: {{json object}}
struct JsonHelper;

impl HelperDef for JsonHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h
            .param(0)
            .map(|v| v.value())
            .ok_or_else(|| render_error("json requires a parameter"))?;

        let json_str = serde_json::to_string(value)
            .map_err(|e| render_error(format!("JSON serialization failed: {}", e)))?;

        out.write(&json_str)?;
        Ok(())
    }
}

/// Pretty-print JSON: {{jsonPretty object}}
struct JsonPrettyHelper;

impl HelperDef for JsonPrettyHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let value = h
            .param(0)
            .map(|v| v.value())
            .ok_or_else(|| render_error("jsonPretty requires a parameter"))?;

        let json_str = serde_json::to_string_pretty(value)
            .map_err(|e| render_error(format!("JSON serialization failed: {}", e)))?;

        out.write(&json_str)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_handlebars() -> Handlebars<'static> {
        let mut hb = Handlebars::new();
        hb.set_strict_mode(true);
        register_builtin_helpers(&mut hb);
        hb
    }

    #[test]
    fn test_uppercase() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("name", "hello world");
        let result = hb.render_template("{{uppercase name}}", &data).unwrap();
        assert_eq!(result, "HELLO WORLD");
    }

    #[test]
    fn test_lowercase() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("name", "HELLO WORLD");
        let result = hb.render_template("{{lowercase name}}", &data).unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_capitalize() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("name", "hello world");
        let result = hb.render_template("{{capitalize name}}", &data).unwrap();
        assert_eq!(result, "Hello world");
    }

    #[test]
    fn test_truncate() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "This is a very long text that should be truncated");
        let result = hb.render_template("{{truncate text 20}}", &data).unwrap();
        assert_eq!(result, "This is a very long ...");
    }

    #[test]
    fn test_truncate_short() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "Short");
        let result = hb.render_template("{{truncate text 20}}", &data).unwrap();
        assert_eq!(result, "Short");
    }

    #[test]
    fn test_trim() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "  hello world  ");
        let result = hb.render_template("{{trim text}}", &data).unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_replace() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "hello world");
        let result = hb
            .render_template("{{replace text \"world\" \"universe\"}}", &data)
            .unwrap();
        assert_eq!(result, "hello universe");
    }

    #[test]
    fn test_format_number() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "price": 123.456 });
        let result = hb
            .render_template("{{formatNumber price 2}}", &data)
            .unwrap();
        assert_eq!(result, "123.46");
    }

    #[test]
    fn test_round() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "value": 3.7 });
        let result = hb.render_template("{{round value}}", &data).unwrap();
        assert_eq!(result, "4");
    }

    #[test]
    fn test_abs() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "value": -5 });
        let result = hb.render_template("{{abs value}}", &data).unwrap();
        assert_eq!(result, "5");
    }

    #[test]
    fn test_default_with_value() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("name", "Alice");
        let result = hb
            .render_template("{{default name \"Unknown\"}}", &data)
            .unwrap();
        assert_eq!(result, "Alice");
    }

    #[test]
    fn test_default_without_value() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "name": null });
        let result = hb
            .render_template("{{default name \"Unknown\"}}", &data)
            .unwrap();
        assert_eq!(result, "Unknown");
    }

    #[test]
    fn test_eq_true() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": "test", "b": "test" });
        let result = hb
            .render_template("{{#if (eq a b)}}equal{{else}}not equal{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "equal");
    }

    #[test]
    fn test_eq_false() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": "test", "b": "other" });
        let result = hb
            .render_template("{{#if (eq a b)}}equal{{else}}not equal{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "not equal");
    }

    #[test]
    fn test_gt() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 10, "b": 5 });
        let result = hb
            .render_template("{{#if (gt a b)}}greater{{else}}not greater{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "greater");
    }

    #[test]
    fn test_json() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "user": { "name": "Alice", "age": 30 } });
        let result = hb.render_template("{{json user}}", &data).unwrap();
        // Parse and compare to avoid key ordering issues
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed, serde_json::json!({ "name": "Alice", "age": 30 }));
    }

    #[test]
    fn test_coalesce() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": null, "b": "", "c": "found" });
        let result = hb.render_template("{{coalesce a b c}}", &data).unwrap();
        assert_eq!(result, "found");
    }

    #[test]
    fn test_ne_true() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": "test", "b": "other" });
        let result = hb
            .render_template("{{#if (ne a b)}}different{{else}}same{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "different");
    }

    #[test]
    fn test_ne_false() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": "test", "b": "test" });
        let result = hb
            .render_template("{{#if (ne a b)}}different{{else}}same{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "same");
    }

    #[test]
    fn test_lt() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 5, "b": 10 });
        let result = hb
            .render_template("{{#if (lt a b)}}less{{else}}not less{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "less");
    }

    #[test]
    fn test_lt_false() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 10, "b": 5 });
        let result = hb
            .render_template("{{#if (lt a b)}}less{{else}}not less{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "not less");
    }

    #[test]
    fn test_gte() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 10, "b": 10 });
        let result = hb
            .render_template("{{#if (gte a b)}}gte{{else}}lt{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "gte");
    }

    #[test]
    fn test_gte_greater() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 15, "b": 10 });
        let result = hb
            .render_template("{{#if (gte a b)}}gte{{else}}lt{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "gte");
    }

    #[test]
    fn test_lte() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 10, "b": 10 });
        let result = hb
            .render_template("{{#if (lte a b)}}lte{{else}}gt{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "lte");
    }

    #[test]
    fn test_lte_less() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "a": 5, "b": 10 });
        let result = hb
            .render_template("{{#if (lte a b)}}lte{{else}}gt{{/if}}", &data)
            .unwrap();
        assert_eq!(result, "lte");
    }

    #[test]
    fn test_format_date_basic() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "date": "2024-03-15" });
        let result = hb
            .render_template("{{formatDate date \"YYYY-MM-DD\"}}", &data)
            .unwrap();
        assert_eq!(result, "2024-03-15");
    }

    #[test]
    fn test_format_date_rfc3339() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "date": "2024-03-15T10:30:00Z" });
        let result = hb
            .render_template("{{formatDate date \"YYYY-MM-DD HH:mm\"}}", &data)
            .unwrap();
        assert_eq!(result, "2024-03-15 10:30");
    }

    #[test]
    fn test_format_date_mmm() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "date": "2024-03-15T10:30:00Z" });
        let result = hb
            .render_template("{{formatDate date \"MMM D, YYYY\"}}", &data)
            .unwrap();
        assert_eq!(result, "Mar 15, 2024");
    }

    #[test]
    fn test_format_date_invalid_fallback() {
        let hb = create_handlebars();
        let data = serde_json::json!({ "date": "not a date" });
        let result = hb
            .render_template("{{formatDate date \"YYYY-MM-DD\"}}", &data)
            .unwrap();
        // Should fall back to original string
        assert_eq!(result, "not a date");
    }

    #[test]
    fn test_today() {
        let hb = create_handlebars();
        let data = serde_json::json!({});
        let result = hb.render_template("{{today}}", &data).unwrap();
        // Should match YYYY-MM-DD format
        assert_eq!(result.len(), 10);
        assert_eq!(&result[4..5], "-");
        assert_eq!(&result[7..8], "-");
    }

    #[test]
    fn test_now() {
        let hb = create_handlebars();
        let data = serde_json::json!({});
        let result = hb.render_template("{{now}}", &data).unwrap();
        // Should end with Z (UTC)
        assert!(result.ends_with('Z'));
        assert!(result.contains('T'));
    }

    #[test]
    fn test_replace_no_match() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "hello world");
        let result = hb
            .render_template("{{replace text \"xyz\" \"abc\"}}", &data)
            .unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_replace_empty_pattern() {
        let hb = create_handlebars();
        let mut data = HashMap::new();
        data.insert("text", "hello");
        let result = hb
            .render_template("{{replace text \"\" \"x\"}}", &data)
            .unwrap();
        // Replacing empty string inserts between each character
        assert_eq!(result, "xhxexlxlxox");
    }
}
