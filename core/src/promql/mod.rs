pub mod eval;
pub mod metric_names;

use promql_parser::parser;

/// Parse a PromQL expression into an AST.
pub fn parse(promql: &str) -> Result<promql_parser::parser::Expr, String> {
    parser::parse(promql)
}

fn default_sanitize_dotted(segment: &str) -> String {
    segment.replace('.', "_")
}

fn disambiguate_dotted(segment: &str) -> String {
    segment.replace('.', "_dot_")
}

/// Collect identifier segments outside string literals.
fn collect_identifier_segments(promql: &str) -> Vec<String> {
    let chars: Vec<char> = promql.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = '"';
    let mut segments = Vec::new();

    while i < len {
        let c = chars[i];

        if !in_string && (c == '"' || c == '`') {
            in_string = true;
            string_char = c;
            i += 1;
            continue;
        }
        if in_string {
            if c == string_char {
                in_string = false;
            }
            i += 1;
            continue;
        }

        if (c.is_alphabetic() || c == '_') && (i == 0 || !chars[i - 1].is_alphanumeric()) {
            let start = i;
            let mut end = i + 1;
            while end < len
                && (chars[end].is_alphanumeric() || chars[end] == '_' || chars[end] == '.')
            {
                end += 1;
            }
            let segment: String = chars[start..end].iter().collect();
            segments.push(segment);
            i = end;
            continue;
        }

        i += 1;
    }

    segments
}

/// Build an injective dotted→sanitised mapping, avoiding collisions with
/// literal underscore identifiers already present in the query.
fn plan_dotted_mappings(segments: &[String]) -> std::collections::HashMap<String, String> {
    use std::collections::{HashMap, HashSet};

    let literals: HashSet<String> = segments
        .iter()
        .filter(|s| !s.contains('.'))
        .cloned()
        .collect();

    let mut used_sanitised: HashSet<String> = literals.clone();
    let mut original_to_sanitised: HashMap<String, String> = HashMap::new();

    for seg in segments.iter().filter(|s| s.contains('.')) {
        if original_to_sanitised.contains_key(seg) {
            continue;
        }
        let default = default_sanitize_dotted(seg);
        let sanitised = if used_sanitised.contains(&default) {
            let alt = disambiguate_dotted(seg);
            if used_sanitised.contains(&alt) {
                // Extremely unlikely: fall back to double-underscore encoding.
                seg.replace('.', "__")
            } else {
                alt
            }
        } else {
            default
        };
        used_sanitised.insert(sanitised.clone());
        original_to_sanitised.insert(seg.clone(), sanitised);
    }

    original_to_sanitised
}

/// Sanitize a single metric or label identifier for use in PromQL / DataFusion names.
///
/// Dotted OTEL names become underscores; other non-alphanumeric characters become `_`.
pub fn sanitize_metric_identifier(segment: &str) -> String {
    if segment.contains('.') {
        default_sanitize_dotted(segment)
    } else {
        segment
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }
}

/// Rewrite OTel-style dotted metric and label names so the standard PromQL
/// parser can handle them.  Dots between alphabetic/underscore characters
/// (outside string literals) are replaced with underscores:
///
///   `system.network.io{host.name=~".*"}` → `system_network_io{host_name=~".*"}`
///
/// When a dotted name would collide with a literal underscore identifier in the
/// same query (e.g. `service.name` vs `service_name`), the dotted form is encoded
/// as `service_dot_name` so both remain distinct.
///
/// Numeric dots (e.g. `0.99`) are left untouched because the character before
/// or after the dot is a digit.
///
/// Returns the sanitised string **and** a sorted, deduplicated list of
/// `(sanitised, original)` name pairs so the caller can reverse-map
/// metric/label names back to their dotted originals when querying storage.
pub fn sanitize_otel_names(promql: &str) -> (String, Vec<(String, String)>) {
    let segments = collect_identifier_segments(promql);
    let dotted_plan = plan_dotted_mappings(&segments);

    let chars: Vec<char> = promql.chars().collect();
    let len = chars.len();
    let mut result = String::with_capacity(len);
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = '"';

    while i < len {
        let c = chars[i];

        if !in_string && (c == '"' || c == '`') {
            in_string = true;
            string_char = c;
            result.push(c);
            i += 1;
            continue;
        }
        if in_string {
            if c == string_char {
                in_string = false;
            }
            result.push(c);
            i += 1;
            continue;
        }

        if (c.is_alphabetic() || c == '_') && (i == 0 || !chars[i - 1].is_alphanumeric()) {
            let start = i;
            let mut end = i + 1;
            while end < len
                && (chars[end].is_alphanumeric() || chars[end] == '_' || chars[end] == '.')
            {
                end += 1;
            }
            let segment: String = chars[start..end].iter().collect();
            if let Some(sanitised) = dotted_plan.get(&segment) {
                result.push_str(sanitised);
            } else {
                result.push_str(&segment);
            }
            i = end;
            continue;
        }

        result.push(c);
        i += 1;
    }

    let mut pairs: Vec<(String, String)> = dotted_plan
        .into_iter()
        .map(|(original, sanitised)| (sanitised, original))
        .collect();
    pairs.sort();
    (result, pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitise_dotted_metric_and_label() {
        let (s, m) = sanitize_otel_names(
            r#"rate(system.network.io{host.name=~".*", direction="receive"}[5m])"#,
        );
        assert_eq!(
            s,
            r#"rate(system_network_io{host_name=~".*", direction="receive"}[5m])"#
        );
        assert!(m
            .iter()
            .any(|(k, v)| k == "system_network_io" && v == "system.network.io"));
        assert!(m.iter().any(|(k, v)| k == "host_name" && v == "host.name"));
    }

    #[test]
    fn preserves_numeric_dots() {
        let (s, m) = sanitize_otel_names("histogram_quantile(0.99, rate(m[5m]))");
        assert_eq!(s, "histogram_quantile(0.99, rate(m[5m]))");
        assert!(m.is_empty());
    }

    #[test]
    fn preserves_string_contents() {
        let (s, _) = sanitize_otel_names(r#"metric{label="foo.bar.baz"}"#);
        assert_eq!(s, r#"metric{label="foo.bar.baz"}"#);
    }

    #[test]
    fn no_dots_passthrough() {
        let (s, m) = sanitize_otel_names("up{job=\"prometheus\"}");
        assert_eq!(s, "up{job=\"prometheus\"}");
        assert!(m.is_empty());
    }

    #[test]
    fn avg_by_with_dotted_names() {
        let (s, _) =
            sanitize_otel_names(r#"avg by (cpu) (system.cpu.utilization{host.name=~".*"})"#);
        assert_eq!(
            s,
            r#"avg by (cpu) (system_cpu_utilization{host_name=~".*"})"#
        );
    }

    #[test]
    fn disambiguates_dotted_vs_literal_label_collision() {
        let (s, m) = sanitize_otel_names(r#"up{service.name="otel", service_name="legacy"}"#);
        assert_eq!(
            s,
            r#"up{service_dot_name="otel", service_name="legacy"}"#
        );
        assert!(m
            .iter()
            .any(|(k, v)| k == "service_dot_name" && v == "service.name"));
        assert!(!m.iter().any(|(k, _)| k == "service_name" && m.len() == 1));
    }

    #[test]
    fn disambiguates_dotted_vs_literal_metric_collision() {
        let (s, m) = sanitize_otel_names("foo.bar + foo_bar");
        assert_eq!(s, "foo_dot_bar + foo_bar");
        assert!(m.iter().any(|(k, v)| k == "foo_dot_bar" && v == "foo.bar"));
    }

    #[test]
    fn reverse_map_is_injective() {
        let (_, m) = sanitize_otel_names(
            r#"rate(system.network.io{host.name="x", service.name="y", service_name="z"})"#,
        );
        let keys: std::collections::HashSet<&str> = m.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys.len(), m.len());
    }

    #[test]
    fn sanitize_metric_identifier_dotted_and_plain() {
        assert_eq!(
            sanitize_metric_identifier("system.network.io"),
            "system_network_io"
        );
        assert_eq!(sanitize_metric_identifier("http_requests_total"), "http_requests_total");
        assert_eq!(sanitize_metric_identifier("foo-bar"), "foo_bar");
    }
}
