use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub schedule: Option<String>,
    pub enabled: bool,
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    Source,
    Transform,
    Sink,
}

impl NodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Transform => "transform",
            Self::Sink => "sink",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "source" => Some(Self::Source),
            "transform" => Some(Self::Transform),
            "sink" => Some(Self::Sink),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineNode {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub node_type: NodeType,
    pub label: String,
    pub config: NodeConfig,
    pub position_x: f32,
    pub position_y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeConfig {
    Source(SourceNodeConfig),
    Transform(TransformNodeConfig),
    Sink(SinkNodeConfig),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ReadStrategy {
    FullSync {
        table: String,
    },
    Incremental {
        table: String,
        cursor_key: String,
    },
    Query {
        sql: String,
    },
    BatchFetch {
        table: String,
        batch_size: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_rows: Option<usize>,
    },
    CdcStream {
        table: String,
    },
    Filter {
        table: String,
        filter: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineMode {
    Batch,
    Streaming,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceNodeConfig {
    pub connector_name: String,
    pub read_strategy: ReadStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformNodeConfig {
    pub udf_name: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkNodeConfig {
    pub connector_name: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEdge {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub from_node_id: Uuid,
    pub to_node_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSummary {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub schedule: Option<String>,
    pub enabled: bool,
    pub mode: String,
    pub node_count: i64,
    pub last_run_status: Option<String>,
    pub last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineRunInfo {
    pub id: Uuid,
    pub pipeline_id: Uuid,
    pub status: String,
    pub trigger: String,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error_message: Option<String>,
    pub step_results: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Pipeline {
    pub fn mode(&self) -> PipelineMode {
        let has_cdc = self.nodes.iter().any(|n| {
            matches!(
                &n.config,
                NodeConfig::Source(s) if matches!(s.read_strategy, ReadStrategy::CdcStream { .. })
            )
        });
        if has_cdc {
            PipelineMode::Streaming
        } else {
            PipelineMode::Batch
        }
    }
}

impl PipelineMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Streaming => "streaming",
        }
    }
}

/// Request payload for creating or updating a pipeline via the API.
#[derive(Debug, Deserialize)]
pub struct PipelineGraphPayload {
    pub name: String,
    pub description: Option<String>,
    pub schedule: Option<String>,
    pub enabled: Option<bool>,
    pub nodes: Vec<NodePayload>,
    pub edges: Vec<EdgePayload>,
}

#[derive(Debug, Deserialize)]
pub struct NodePayload {
    pub id: Uuid,
    pub node_type: NodeType,
    pub label: String,
    pub config: NodeConfig,
    pub position_x: f32,
    pub position_y: f32,
}

#[derive(Debug, Deserialize)]
pub struct EdgePayload {
    pub from_node_id: Uuid,
    pub to_node_id: Uuid,
}

impl PipelineGraphPayload {
    pub fn compute_mode(&self) -> PipelineMode {
        let has_cdc = self.nodes.iter().any(|n| {
            matches!(
                &n.config,
                NodeConfig::Source(s) if matches!(s.read_strategy, ReadStrategy::CdcStream { .. })
            )
        });
        if has_cdc {
            PipelineMode::Streaming
        } else {
            PipelineMode::Batch
        }
    }
}
