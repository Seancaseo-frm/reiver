//! Flame graph generation from OpenTelemetry profiling data
//!
//! This module converts OTLP v1development profile data (stored as
//! base64-encoded protobuf) into flame graph data structures for
//! frontend visualization.

use opentelemetry_proto::tonic::profiles::v1development::{Profile, ProfilesDictionary};
use serde::{Deserialize, Serialize};

/// Flame graph node representing a stack frame
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlameGraphNode {
    /// Function name (or location)
    pub name: String,
    /// Total value (CPU samples, memory allocations, etc.)
    pub value: u64,
    /// Children nodes (callers or callees depending on direction)
    pub children: Vec<FlameGraphNode>,
    /// Source filename, if resolved from the profile dictionary
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Function name without filename decoration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    /// Line number in source file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_number: Option<i64>,
}

/// Flame graph data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlameGraph {
    /// Root node of the flame graph
    pub root: FlameGraphNode,
    /// Total value across all nodes
    pub total_value: u64,
    /// Metadata about the profile
    pub metadata: FlameGraphMetadata,
}

/// Metadata about the flame graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlameGraphMetadata {
    /// Profile type (cpu, memory, etc.)
    pub profile_type: String,
    /// Number of samples
    pub sample_count: u64,
    /// Duration in nanoseconds
    pub duration_nano: u64,
    /// Period (sampling period)
    pub period: i64,
}

/// Generate a flame graph from OTLP profile data.
///
/// Expects base64-encoded protobuf bytes for both profile and dictionary.
pub fn generate_flame_graph(
    profile_data: &[u8],
    dictionary_data: &[u8],
) -> Result<FlameGraph, String> {
    use base64::Engine as _;
    use prost::Message;

    let b64 = base64::engine::general_purpose::STANDARD;

    let profile_bytes = b64
        .decode(profile_data)
        .map_err(|e| format!("Failed to base64-decode profile_data: {}", e))?;
    let profile: Profile = Message::decode(profile_bytes.as_slice())
        .map_err(|e| format!("Failed to decode profile protobuf: {}", e))?;

    let dict_bytes = b64
        .decode(dictionary_data)
        .map_err(|e| format!("Failed to base64-decode dictionary_data: {}", e))?;
    let dictionary: ProfilesDictionary = Message::decode(dict_bytes.as_slice())
        .map_err(|e| format!("Failed to decode dictionary protobuf: {}", e))?;

    // Convenience accessors into the dictionary tables
    let string_table = &dictionary.string_table;
    let function_table = &dictionary.function_table;
    let location_table = &dictionary.location_table;
    let stack_table = &dictionary.stack_table;

    // Helper: resolve a function index to (name, filename)
    let resolve_function = |func_idx: i32| -> (String, Option<String>) {
        if func_idx <= 0 || (func_idx as usize) >= function_table.len() {
            return ("<unknown>".to_string(), None);
        }
        let func = &function_table[func_idx as usize];
        let name = string_table
            .get(func.name_strindex as usize)
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_else(|| "<unknown>".to_string());
        let filename = string_table
            .get(func.filename_strindex as usize)
            .filter(|s| !s.is_empty())
            .cloned();
        (name, filename)
    };

    // Resolved location metadata for building flamegraph nodes.
    struct ResolvedLocation {
        display_name: String,
        filename: Option<String>,
        function_name: Option<String>,
        line_number: Option<i64>,
    }

    // Helper: resolve a location index to display name and source metadata.
    // A location can have multiple lines (inlined functions); we use the first.
    let resolve_location = |loc_idx: i32| -> ResolvedLocation {
        if loc_idx <= 0 || (loc_idx as usize) >= location_table.len() {
            return ResolvedLocation {
                display_name: "<unknown>".to_string(),
                filename: None,
                function_name: None,
                line_number: None,
            };
        }
        let loc = &location_table[loc_idx as usize];
        if let Some(first_line) = loc.line.first() {
            let (name, filename) = resolve_function(first_line.function_index);
            let display_name = if let Some(ref fname) = filename {
                format!("{} ({})", name, fname)
            } else {
                name.clone()
            };
            let line_number = if first_line.line > 0 {
                Some(first_line.line)
            } else {
                None
            };
            ResolvedLocation {
                display_name,
                filename,
                function_name: Some(name),
                line_number,
            }
        } else {
            ResolvedLocation {
                display_name: format!("0x{:x}", loc.address),
                filename: None,
                function_name: None,
                line_number: None,
            }
        }
    };

    // Build the flame graph tree from samples
    let mut root = FlameGraphNode {
        name: "root".to_string(),
        value: 0,
        children: Vec::new(),
        filename: None,
        function_name: None,
        line_number: None,
    };
    let mut total_value: u64 = 0;

    for sample in &profile.sample {
        // Determine the sample's value (use first value or count of timestamps or 1)
        let value: u64 = if !sample.values.is_empty() {
            sample
                .values
                .iter()
                .map(|v| (*v).max(0) as u64)
                .sum::<u64>()
                .max(1)
        } else if !sample.timestamps_unix_nano.is_empty() {
            sample.timestamps_unix_nano.len() as u64
        } else {
            1
        };

        total_value += value;
        root.value += value;

        // Resolve the stack via stack_index -> Stack -> location_indices
        let location_indices: &[i32] =
            if sample.stack_index > 0 && (sample.stack_index as usize) < stack_table.len() {
                &stack_table[sample.stack_index as usize].location_indices
            } else {
                &[]
            };

        // The first location in the stack is the leaf frame; we want root-first
        // traversal so iterate in reverse.
        let mut current = &mut root;
        for &loc_idx in location_indices.iter().rev() {
            let resolved = resolve_location(loc_idx);

            let child_idx = current
                .children
                .iter()
                .position(|c| c.name == resolved.display_name);
            if let Some(idx) = child_idx {
                current.children[idx].value += value;
                current = &mut current.children[idx];
            } else {
                current.children.push(FlameGraphNode {
                    name: resolved.display_name,
                    value,
                    children: Vec::new(),
                    filename: resolved.filename,
                    function_name: resolved.function_name,
                    line_number: resolved.line_number,
                });
                current = current.children.last_mut().unwrap();
            }
        }
    }

    // Sort children by value (descending) for a nicer visual
    sort_flame_graph_nodes(&mut root);

    // Extract metadata
    let profile_type = profile
        .sample_type
        .as_ref()
        .map(|st| {
            string_table
                .get(st.type_strindex as usize)
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(|| "cpu".to_string())
        })
        .unwrap_or_else(|| "cpu".to_string());

    let metadata = FlameGraphMetadata {
        profile_type,
        sample_count: total_value,
        duration_nano: profile.duration_nano,
        period: profile.period,
    };

    Ok(FlameGraph {
        root,
        total_value,
        metadata,
    })
}

/// Sort flame graph nodes by value (descending)
fn sort_flame_graph_nodes(node: &mut FlameGraphNode) {
    node.children.sort_by(|a, b| b.value.cmp(&a.value));
    for child in &mut node.children {
        sort_flame_graph_nodes(child);
    }
}

/// Merge multiple flame graph trees into one by summing values for matching paths.
pub fn merge_flame_graphs(trees: Vec<FlameGraphNode>) -> FlameGraphNode {
    let mut merged = FlameGraphNode {
        name: "root".to_string(),
        value: 0,
        children: Vec::new(),
        filename: None,
        function_name: None,
        line_number: None,
    };

    for tree in trees {
        merged.value += tree.value;
        merge_children(&mut merged.children, tree.children);
    }

    sort_flame_graph_nodes(&mut merged);
    merged
}

fn merge_children(target: &mut Vec<FlameGraphNode>, source: Vec<FlameGraphNode>) {
    for src_child in source {
        if let Some(existing) = target.iter_mut().find(|c| c.name == src_child.name) {
            existing.value += src_child.value;
            merge_children(&mut existing.children, src_child.children);
        } else {
            target.push(src_child);
        }
    }
}

/// Diff flame graph node for version comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffFlameGraphNode {
    pub name: String,
    pub value_a: u64,
    pub value_b: u64,
    pub diff: i64,
    pub children: Vec<DiffFlameGraphNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_number: Option<i64>,
}

/// Diff flame graph wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffFlameGraph {
    pub root: DiffFlameGraphNode,
    pub total_value_a: u64,
    pub total_value_b: u64,
}

/// Produce a diff tree from two flame graph trees.
pub fn diff_flame_graphs(a: &FlameGraphNode, b: &FlameGraphNode) -> DiffFlameGraph {
    let root = diff_nodes(a, b);
    DiffFlameGraph {
        total_value_a: a.value,
        total_value_b: b.value,
        root,
    }
}

fn diff_nodes(a: &FlameGraphNode, b: &FlameGraphNode) -> DiffFlameGraphNode {
    let mut children = Vec::new();

    for a_child in &a.children {
        if let Some(b_child) = b.children.iter().find(|c| c.name == a_child.name) {
            children.push(diff_nodes(a_child, b_child));
        } else {
            children.push(node_as_diff(a_child, true));
        }
    }

    for b_child in &b.children {
        if !a.children.iter().any(|c| c.name == b_child.name) {
            children.push(node_as_diff(b_child, false));
        }
    }

    children.sort_by(|x, y| {
        let max_x = x.value_a.max(x.value_b);
        let max_y = y.value_a.max(y.value_b);
        max_y.cmp(&max_x)
    });

    DiffFlameGraphNode {
        name: a.name.clone(),
        value_a: a.value,
        value_b: b.value,
        diff: b.value as i64 - a.value as i64,
        children,
        filename: a.filename.clone().or_else(|| b.filename.clone()),
        function_name: a.function_name.clone().or_else(|| b.function_name.clone()),
        line_number: a.line_number.or(b.line_number),
    }
}

fn node_as_diff(node: &FlameGraphNode, is_a: bool) -> DiffFlameGraphNode {
    let children = node
        .children
        .iter()
        .map(|c| node_as_diff(c, is_a))
        .collect();

    if is_a {
        DiffFlameGraphNode {
            name: node.name.clone(),
            value_a: node.value,
            value_b: 0,
            diff: -(node.value as i64),
            children,
            filename: node.filename.clone(),
            function_name: node.function_name.clone(),
            line_number: node.line_number,
        }
    } else {
        DiffFlameGraphNode {
            name: node.name.clone(),
            value_a: 0,
            value_b: node.value,
            diff: node.value as i64,
            children,
            filename: node.filename.clone(),
            function_name: node.function_name.clone(),
            line_number: node.line_number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use opentelemetry_proto::tonic::profiles::v1development as proto;
    use prost::Message;

    fn to_b64<T: Message>(val: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        val.encode(&mut buf).expect("encoding should not fail");
        base64::engine::general_purpose::STANDARD
            .encode(&buf)
            .into_bytes()
    }

    #[test]
    fn test_generate_flame_graph_otlp_v1dev() {
        // Build typed structs exactly as the OTLP ingestion path does.
        let profile = proto::Profile {
            sample_type: Some(proto::ValueType {
                type_strindex: 1,
                unit_strindex: 2,
                aggregation_temporality: 0,
            }),
            sample: vec![proto::Sample {
                stack_index: 1,
                values: vec![10],
                ..Default::default()
            }],
            time_unix_nano: 1_000_000_000,
            duration_nano: 5_000_000_000,
            period: 10_000_000,
            ..Default::default()
        };

        let dictionary = proto::ProfilesDictionary {
            string_table: vec![
                "".into(),            // 0
                "cpu".into(),         // 1
                "nanoseconds".into(), // 2
                "main".into(),        // 3
                "app.rs".into(),      // 4
                "helper".into(),      // 5
            ],
            function_table: vec![
                // index 0: null/default
                proto::Function::default(),
                // index 1: main
                proto::Function {
                    name_strindex: 3,
                    filename_strindex: 4,
                    start_line: 1,
                    ..Default::default()
                },
                // index 2: helper
                proto::Function {
                    name_strindex: 5,
                    filename_strindex: 4,
                    start_line: 15,
                    ..Default::default()
                },
            ],
            location_table: vec![
                // index 0: null/default
                proto::Location::default(),
                // index 1: main
                proto::Location {
                    address: 100,
                    line: vec![proto::Line {
                        function_index: 1,
                        line: 10,
                        column: 0,
                    }],
                    ..Default::default()
                },
                // index 2: helper
                proto::Location {
                    address: 200,
                    line: vec![proto::Line {
                        function_index: 2,
                        line: 20,
                        column: 0,
                    }],
                    ..Default::default()
                },
            ],
            stack_table: vec![
                // index 0: null/default
                proto::Stack::default(),
                // index 1: helper (leaf) -> main (root)
                proto::Stack {
                    location_indices: vec![2, 1],
                },
            ],
            ..Default::default()
        };

        let profile_bytes = to_b64(&profile);
        let dict_bytes = to_b64(&dictionary);

        let result = generate_flame_graph(&profile_bytes, &dict_bytes);
        assert!(
            result.is_ok(),
            "generate_flame_graph failed: {:?}",
            result.err()
        );

        let fg = result.unwrap();
        assert_eq!(fg.total_value, 10);
        assert_eq!(fg.root.value, 10);
        assert_eq!(fg.root.children.len(), 1);

        // root -> main (app.rs) -> helper (app.rs)
        let main_node = &fg.root.children[0];
        assert_eq!(main_node.name, "main (app.rs)");
        assert_eq!(main_node.value, 10);
        assert_eq!(main_node.children.len(), 1);
        // Verify source location metadata
        assert_eq!(main_node.filename.as_deref(), Some("app.rs"));
        assert_eq!(main_node.function_name.as_deref(), Some("main"));
        assert_eq!(main_node.line_number, Some(10));

        let helper_node = &main_node.children[0];
        assert_eq!(helper_node.name, "helper (app.rs)");
        assert_eq!(helper_node.value, 10);
        assert_eq!(helper_node.filename.as_deref(), Some("app.rs"));
        assert_eq!(helper_node.function_name.as_deref(), Some("helper"));
        assert_eq!(helper_node.line_number, Some(20));

        // Root node should have no source metadata
        assert!(fg.root.filename.is_none());
        assert!(fg.root.function_name.is_none());
        assert!(fg.root.line_number.is_none());
    }

    #[test]
    fn test_empty_profile() {
        let profile = proto::Profile::default();
        let dictionary = proto::ProfilesDictionary {
            string_table: vec!["".into()],
            ..Default::default()
        };

        let result = generate_flame_graph(&to_b64(&profile), &to_b64(&dictionary));
        assert!(result.is_ok(), "empty profile failed: {:?}", result.err());
        let fg = result.unwrap();
        assert_eq!(fg.total_value, 0);
        assert!(fg.root.children.is_empty());
    }

    #[test]
    fn test_multiple_samples_same_stack() {
        let profile = proto::Profile {
            sample: vec![
                proto::Sample {
                    stack_index: 1,
                    values: vec![5],
                    ..Default::default()
                },
                proto::Sample {
                    stack_index: 1,
                    values: vec![3],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let dictionary = proto::ProfilesDictionary {
            string_table: vec!["".into(), "foo".into()],
            function_table: vec![
                proto::Function::default(),
                proto::Function {
                    name_strindex: 1,
                    ..Default::default()
                },
            ],
            location_table: vec![
                proto::Location::default(),
                proto::Location {
                    line: vec![proto::Line {
                        function_index: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            stack_table: vec![
                proto::Stack::default(),
                proto::Stack {
                    location_indices: vec![1],
                },
            ],
            ..Default::default()
        };

        let fg = generate_flame_graph(&to_b64(&profile), &to_b64(&dictionary)).unwrap();
        assert_eq!(fg.total_value, 8);
        assert_eq!(fg.root.children.len(), 1);
        assert_eq!(fg.root.children[0].name, "foo");
        assert_eq!(fg.root.children[0].value, 8);
    }
}
