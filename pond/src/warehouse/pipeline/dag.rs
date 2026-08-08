use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;
use uuid::Uuid;

use super::types::{NodeConfig, NodeType, PipelineEdge, PipelineNode, ReadStrategy};

#[derive(Debug, Error)]
pub enum DagValidationError {
    #[error("cycle detected in pipeline graph")]
    CycleDetected,

    #[error("source node '{0}' must not have incoming edges")]
    SourceHasIncoming(Uuid),

    #[error("sink node '{0}' must not have outgoing edges")]
    SinkHasOutgoing(Uuid),

    #[error("transform node '{0}' has no incoming edges")]
    TransformNoInput(Uuid),

    #[error("transform node '{0}' has no outgoing edges")]
    TransformNoOutput(Uuid),

    #[error("sink node '{0}' has no incoming edges")]
    SinkNoInput(Uuid),

    #[error("edge references unknown node '{0}'")]
    UnknownNode(Uuid),

    #[error("pipeline has no nodes")]
    Empty,

    #[error("pipeline has no source nodes")]
    NoSources,

    #[error("pipeline has no sink nodes")]
    NoSinks,

    #[error("pipeline mixes CdcStream and batch source strategies; all sources must be the same mode")]
    MixedSourceModes,
}

/// Validate the DAG structure and return a topologically sorted list of node IDs.
///
/// Validation rules:
/// - No cycles (Kahn's algorithm detects these)
/// - Source nodes have zero incoming edges
/// - Sink nodes have zero outgoing edges
/// - Transform nodes have at least one incoming and one outgoing edge
/// - Sink nodes have at least one incoming edge
/// - All edge endpoints reference nodes in the graph
pub fn topological_sort(
    nodes: &[PipelineNode],
    edges: &[PipelineEdge],
) -> Result<Vec<Uuid>, DagValidationError> {
    if nodes.is_empty() {
        return Err(DagValidationError::Empty);
    }

    let node_ids: HashSet<Uuid> = nodes.iter().map(|n| n.id).collect();
    let node_types: HashMap<Uuid, NodeType> = nodes.iter().map(|n| (n.id, n.node_type)).collect();

    for edge in edges {
        if !node_ids.contains(&edge.from_node_id) {
            return Err(DagValidationError::UnknownNode(edge.from_node_id));
        }
        if !node_ids.contains(&edge.to_node_id) {
            return Err(DagValidationError::UnknownNode(edge.to_node_id));
        }
    }

    let has_source = nodes.iter().any(|n| n.node_type == NodeType::Source);
    let has_sink = nodes.iter().any(|n| n.node_type == NodeType::Sink);
    if !has_source {
        return Err(DagValidationError::NoSources);
    }
    if !has_sink {
        return Err(DagValidationError::NoSinks);
    }

    let mut has_cdc = false;
    let mut has_batch = false;
    for node in nodes {
        if let NodeConfig::Source(src) = &node.config {
            if matches!(src.read_strategy, ReadStrategy::CdcStream { .. }) {
                has_cdc = true;
            } else {
                has_batch = true;
            }
        }
    }
    if has_cdc && has_batch {
        return Err(DagValidationError::MixedSourceModes);
    }

    let mut in_degree: HashMap<Uuid, usize> = node_ids.iter().map(|id| (*id, 0)).collect();
    let mut adjacency: HashMap<Uuid, Vec<Uuid>> =
        node_ids.iter().map(|id| (*id, Vec::new())).collect();
    let mut incoming: HashMap<Uuid, Vec<Uuid>> =
        node_ids.iter().map(|id| (*id, Vec::new())).collect();

    for edge in edges {
        adjacency
            .get_mut(&edge.from_node_id)
            .unwrap()
            .push(edge.to_node_id);
        incoming
            .get_mut(&edge.to_node_id)
            .unwrap()
            .push(edge.from_node_id);
        *in_degree.get_mut(&edge.to_node_id).unwrap() += 1;
    }

    for node in nodes {
        let has_incoming = !incoming[&node.id].is_empty();
        let has_outgoing = !adjacency[&node.id].is_empty();

        match node.node_type {
            NodeType::Source => {
                if has_incoming {
                    return Err(DagValidationError::SourceHasIncoming(node.id));
                }
            }
            NodeType::Transform => {
                if !has_incoming {
                    return Err(DagValidationError::TransformNoInput(node.id));
                }
                if !has_outgoing {
                    return Err(DagValidationError::TransformNoOutput(node.id));
                }
            }
            NodeType::Sink => {
                if has_outgoing {
                    return Err(DagValidationError::SinkHasOutgoing(node.id));
                }
                if !has_incoming {
                    return Err(DagValidationError::SinkNoInput(node.id));
                }
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<Uuid> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(id, _)| *id)
        .collect();

    let mut sorted = Vec::with_capacity(nodes.len());

    while let Some(node_id) = queue.pop_front() {
        sorted.push(node_id);
        for &neighbor in &adjacency[&node_id] {
            let deg = in_degree.get_mut(&neighbor).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    if sorted.len() != nodes.len() {
        return Err(DagValidationError::CycleDetected);
    }

    // Stable ordering: sources first, then transforms, then sinks
    sorted.sort_by_key(|id| match node_types[id] {
        NodeType::Source => 0,
        NodeType::Transform => 1,
        NodeType::Sink => 2,
    });

    Ok(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warehouse::pipeline::types::{
        NodeConfig, ReadStrategy, SinkNodeConfig, SourceNodeConfig, TransformNodeConfig,
    };

    fn source_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
        PipelineNode {
            id,
            pipeline_id,
            node_type: NodeType::Source,
            label: "src".to_string(),
            config: NodeConfig::Source(SourceNodeConfig {
                connector_name: "pg".to_string(),
                read_strategy: ReadStrategy::FullSync {
                    table: "t".to_string(),
                },
            }),
            position_x: 0.0,
            position_y: 0.0,
        }
    }

    fn cdc_source_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
        PipelineNode {
            id,
            pipeline_id,
            node_type: NodeType::Source,
            label: "cdc_src".to_string(),
            config: NodeConfig::Source(SourceNodeConfig {
                connector_name: "pg".to_string(),
                read_strategy: ReadStrategy::CdcStream {
                    table: "t".to_string(),
                },
            }),
            position_x: 0.0,
            position_y: 0.0,
        }
    }

    fn transform_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
        PipelineNode {
            id,
            pipeline_id,
            node_type: NodeType::Transform,
            label: "udf".to_string(),
            config: NodeConfig::Transform(TransformNodeConfig {
                udf_name: "my_udf".to_string(),
                params: Default::default(),
            }),
            position_x: 0.0,
            position_y: 0.0,
        }
    }

    fn sink_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
        PipelineNode {
            id,
            pipeline_id,
            node_type: NodeType::Sink,
            label: "sink".to_string(),
            config: NodeConfig::Sink(SinkNodeConfig {
                connector_name: "ch".to_string(),
                table: "out".to_string(),
            }),
            position_x: 0.0,
            position_y: 0.0,
        }
    }

    fn edge(pipeline_id: Uuid, from: Uuid, to: Uuid) -> PipelineEdge {
        PipelineEdge {
            id: Uuid::new_v4(),
            pipeline_id,
            from_node_id: from,
            to_node_id: to,
        }
    }

    #[test]
    fn linear_pipeline_sorts() {
        let pid = Uuid::new_v4();
        let s = Uuid::new_v4();
        let t = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            source_node(s, pid),
            transform_node(t, pid),
            sink_node(k, pid),
        ];
        let edges = vec![edge(pid, s, t), edge(pid, t, k)];

        let sorted = topological_sort(&nodes, &edges).unwrap();
        assert_eq!(sorted.len(), 3);
        let s_pos = sorted.iter().position(|id| *id == s).unwrap();
        let t_pos = sorted.iter().position(|id| *id == t).unwrap();
        let k_pos = sorted.iter().position(|id| *id == k).unwrap();
        assert!(s_pos < t_pos);
        assert!(t_pos < k_pos);
    }

    #[test]
    fn fan_in_pipeline() {
        let pid = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        let t = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            source_node(s1, pid),
            source_node(s2, pid),
            transform_node(t, pid),
            sink_node(k, pid),
        ];
        let edges = vec![edge(pid, s1, t), edge(pid, s2, t), edge(pid, t, k)];

        let sorted = topological_sort(&nodes, &edges).unwrap();
        assert_eq!(sorted.len(), 4);
    }

    #[test]
    fn cycle_detected() {
        let pid = Uuid::new_v4();
        let s = Uuid::new_v4();
        let t1 = Uuid::new_v4();
        let t2 = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            source_node(s, pid),
            transform_node(t1, pid),
            transform_node(t2, pid),
            sink_node(k, pid),
        ];
        let edges = vec![
            edge(pid, s, t1),
            edge(pid, t1, t2),
            edge(pid, t2, t1), // cycle
            edge(pid, t2, k),
        ];

        let result = topological_sort(&nodes, &edges);
        assert!(matches!(result, Err(DagValidationError::CycleDetected)));
    }

    #[test]
    fn empty_pipeline() {
        let result = topological_sort(&[], &[]);
        assert!(matches!(result, Err(DagValidationError::Empty)));
    }

    #[test]
    fn source_with_incoming_rejected() {
        let pid = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            source_node(s1, pid),
            source_node(s2, pid),
            sink_node(k, pid),
        ];
        let edges = vec![edge(pid, s1, s2), edge(pid, s2, k)];

        let result = topological_sort(&nodes, &edges);
        assert!(matches!(
            result,
            Err(DagValidationError::SourceHasIncoming(_))
        ));
    }

    #[test]
    fn sink_with_outgoing_rejected() {
        let pid = Uuid::new_v4();
        let s = Uuid::new_v4();
        let k = Uuid::new_v4();
        let t = Uuid::new_v4();

        let nodes = vec![
            source_node(s, pid),
            sink_node(k, pid),
            transform_node(t, pid),
        ];
        let edges = vec![edge(pid, s, k), edge(pid, k, t)];

        let result = topological_sort(&nodes, &edges);
        assert!(matches!(
            result,
            Err(DagValidationError::SinkHasOutgoing(_))
        ));
    }

    #[test]
    fn mixed_cdc_and_batch_sources_rejected() {
        let pid = Uuid::new_v4();
        let s_batch = Uuid::new_v4();
        let s_cdc = Uuid::new_v4();
        let t = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            source_node(s_batch, pid),
            cdc_source_node(s_cdc, pid),
            transform_node(t, pid),
            sink_node(k, pid),
        ];
        let edges = vec![
            edge(pid, s_batch, t),
            edge(pid, s_cdc, t),
            edge(pid, t, k),
        ];

        let result = topological_sort(&nodes, &edges);
        assert!(matches!(
            result,
            Err(DagValidationError::MixedSourceModes)
        ));
    }

    #[test]
    fn all_cdc_sources_accepted() {
        let pid = Uuid::new_v4();
        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        let t = Uuid::new_v4();
        let k = Uuid::new_v4();

        let nodes = vec![
            cdc_source_node(s1, pid),
            cdc_source_node(s2, pid),
            transform_node(t, pid),
            sink_node(k, pid),
        ];
        let edges = vec![
            edge(pid, s1, t),
            edge(pid, s2, t),
            edge(pid, t, k),
        ];

        let result = topological_sort(&nodes, &edges);
        assert!(result.is_ok());
    }
}
