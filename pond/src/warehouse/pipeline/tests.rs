use uuid::Uuid;

use super::dag::{topological_sort, DagValidationError};
use super::events::EventStore;
use super::types::*;

fn make_source_node(id: Uuid, pipeline_id: Uuid, strategy: ReadStrategy) -> PipelineNode {
    PipelineNode {
        id,
        pipeline_id,
        node_type: NodeType::Source,
        label: "src".to_string(),
        config: NodeConfig::Source(SourceNodeConfig {
            connector_name: "pg".to_string(),
            read_strategy: strategy,
        }),
        position_x: 0.0,
        position_y: 0.0,
    }
}

fn make_transform_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
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

fn make_sink_node(id: Uuid, pipeline_id: Uuid) -> PipelineNode {
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

fn make_edge(pipeline_id: Uuid, from: Uuid, to: Uuid) -> PipelineEdge {
    PipelineEdge {
        id: Uuid::new_v4(),
        pipeline_id,
        from_node_id: from,
        to_node_id: to,
    }
}

fn make_pipeline(
    nodes: Vec<PipelineNode>,
    edges: Vec<PipelineEdge>,
) -> Pipeline {
    let pid = nodes.first().map(|n| n.pipeline_id).unwrap_or_else(Uuid::new_v4);
    Pipeline {
        id: pid,
        project_id: Uuid::new_v4(),
        name: "test_pipeline".to_string(),
        description: None,
        schedule: None,
        enabled: true,
        nodes,
        edges,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

// ============================================================================
// Read strategy serialization
// ============================================================================

#[test]
fn read_strategy_full_sync_serialization() {
    let strategy = ReadStrategy::FullSync {
        table: "orders".to_string(),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "full_sync");
    assert_eq!(json["table"], "orders");

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn read_strategy_incremental_serialization() {
    let strategy = ReadStrategy::Incremental {
        table: "events".to_string(),
        cursor_key: "updated_at".to_string(),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "incremental");
    assert_eq!(json["cursor_key"], "updated_at");

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn read_strategy_query_serialization() {
    let strategy = ReadStrategy::Query {
        sql: "SELECT * FROM t".to_string(),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "query");

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn read_strategy_batch_fetch_serialization() {
    let strategy = ReadStrategy::BatchFetch {
        table: "t".to_string(),
        batch_size: 1000,
        max_rows: Some(5000),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "batch_fetch");
    assert_eq!(json["batch_size"], 1000);
    assert_eq!(json["max_rows"], 5000);

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn read_strategy_cdc_stream_serialization() {
    let strategy = ReadStrategy::CdcStream {
        table: "orders".to_string(),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "cdc_stream");

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn read_strategy_filter_serialization() {
    let strategy = ReadStrategy::Filter {
        table: "orders".to_string(),
        filter: "status = 'active'".to_string(),
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert_eq!(json["strategy"], "filter");
    assert_eq!(json["filter"], "status = 'active'");

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}

#[test]
fn source_node_config_serialization() {
    let config = SourceNodeConfig {
        connector_name: "pg_main".to_string(),
        read_strategy: ReadStrategy::FullSync {
            table: "users".to_string(),
        },
    };
    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(json["connector_name"], "pg_main");
    assert_eq!(json["read_strategy"]["strategy"], "full_sync");

    let deserialized: SourceNodeConfig = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized.connector_name, "pg_main");
    assert_eq!(deserialized.read_strategy, config.read_strategy);
}

// ============================================================================
// Pipeline mode detection
// ============================================================================

#[test]
fn pipeline_mode_batch_for_full_sync() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let pipeline = make_pipeline(
        vec![
            make_source_node(s, pid, ReadStrategy::FullSync { table: "t".into() }),
            make_transform_node(t, pid),
            make_sink_node(k, pid),
        ],
        vec![make_edge(pid, s, t), make_edge(pid, t, k)],
    );

    assert_eq!(pipeline.mode(), PipelineMode::Batch);
}

#[test]
fn pipeline_mode_batch_for_incremental() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let pipeline = make_pipeline(
        vec![
            make_source_node(
                s,
                pid,
                ReadStrategy::Incremental {
                    table: "t".into(),
                    cursor_key: "id".into(),
                },
            ),
            make_transform_node(t, pid),
            make_sink_node(k, pid),
        ],
        vec![make_edge(pid, s, t), make_edge(pid, t, k)],
    );

    assert_eq!(pipeline.mode(), PipelineMode::Batch);
}

#[test]
fn pipeline_mode_streaming_for_cdc() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let pipeline = make_pipeline(
        vec![
            make_source_node(s, pid, ReadStrategy::CdcStream { table: "t".into() }),
            make_transform_node(t, pid),
            make_sink_node(k, pid),
        ],
        vec![make_edge(pid, s, t), make_edge(pid, t, k)],
    );

    assert_eq!(pipeline.mode(), PipelineMode::Streaming);
}

#[test]
fn pipeline_mode_batch_for_query() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let pipeline = make_pipeline(
        vec![
            make_source_node(
                s,
                pid,
                ReadStrategy::Query {
                    sql: "SELECT 1".into(),
                },
            ),
            make_transform_node(t, pid),
            make_sink_node(k, pid),
        ],
        vec![make_edge(pid, s, t), make_edge(pid, t, k)],
    );

    assert_eq!(pipeline.mode(), PipelineMode::Batch);
}

#[test]
fn pipeline_mode_batch_for_filter() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let pipeline = make_pipeline(
        vec![
            make_source_node(
                s,
                pid,
                ReadStrategy::Filter {
                    table: "t".into(),
                    filter: "x > 0".into(),
                },
            ),
            make_transform_node(t, pid),
            make_sink_node(k, pid),
        ],
        vec![make_edge(pid, s, t), make_edge(pid, t, k)],
    );

    assert_eq!(pipeline.mode(), PipelineMode::Batch);
}

// ============================================================================
// Mixed pipeline rejection
// ============================================================================

#[test]
fn mixed_cdc_and_batch_sources_rejected() {
    let pid = Uuid::new_v4();
    let s_batch = Uuid::new_v4();
    let s_cdc = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let nodes = vec![
        make_source_node(s_batch, pid, ReadStrategy::FullSync { table: "a".into() }),
        make_source_node(s_cdc, pid, ReadStrategy::CdcStream { table: "b".into() }),
        make_transform_node(t, pid),
        make_sink_node(k, pid),
    ];
    let edges = vec![
        make_edge(pid, s_batch, t),
        make_edge(pid, s_cdc, t),
        make_edge(pid, t, k),
    ];

    let result = topological_sort(&nodes, &edges);
    assert!(matches!(result, Err(DagValidationError::MixedSourceModes)));
}

#[test]
fn all_cdc_sources_accepted() {
    let pid = Uuid::new_v4();
    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let nodes = vec![
        make_source_node(s1, pid, ReadStrategy::CdcStream { table: "a".into() }),
        make_source_node(s2, pid, ReadStrategy::CdcStream { table: "b".into() }),
        make_transform_node(t, pid),
        make_sink_node(k, pid),
    ];
    let edges = vec![
        make_edge(pid, s1, t),
        make_edge(pid, s2, t),
        make_edge(pid, t, k),
    ];

    let result = topological_sort(&nodes, &edges);
    assert!(result.is_ok());
}

#[test]
fn all_batch_sources_accepted() {
    let pid = Uuid::new_v4();
    let s1 = Uuid::new_v4();
    let s2 = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let nodes = vec![
        make_source_node(s1, pid, ReadStrategy::FullSync { table: "a".into() }),
        make_source_node(
            s2,
            pid,
            ReadStrategy::Incremental {
                table: "b".into(),
                cursor_key: "id".into(),
            },
        ),
        make_transform_node(t, pid),
        make_sink_node(k, pid),
    ];
    let edges = vec![
        make_edge(pid, s1, t),
        make_edge(pid, s2, t),
        make_edge(pid, t, k),
    ];

    let result = topological_sort(&nodes, &edges);
    assert!(result.is_ok());
}

// ============================================================================
// Event filter matching
// ============================================================================

#[test]
fn event_filter_empty_matches_all() {
    let payload = serde_json::json!({"table": "orders", "count": 42});
    let filter = serde_json::json!({});
    assert!(EventStore::matches_filter(&payload, &filter));
}

#[test]
fn event_filter_exact_match() {
    let payload = serde_json::json!({"table": "orders", "count": 42});
    let filter = serde_json::json!({"table": "orders"});
    assert!(EventStore::matches_filter(&payload, &filter));
}

#[test]
fn event_filter_no_match_different_value() {
    let payload = serde_json::json!({"table": "users"});
    let filter = serde_json::json!({"table": "orders"});
    assert!(!EventStore::matches_filter(&payload, &filter));
}

#[test]
fn event_filter_no_match_missing_key() {
    let payload = serde_json::json!({"count": 42});
    let filter = serde_json::json!({"table": "orders"});
    assert!(!EventStore::matches_filter(&payload, &filter));
}

#[test]
fn event_filter_null_filter_matches_all() {
    let payload = serde_json::json!({"table": "orders"});
    let filter = serde_json::Value::Null;
    assert!(EventStore::matches_filter(&payload, &filter));
}

#[test]
fn event_filter_multi_key_all_must_match() {
    let payload = serde_json::json!({"table": "orders", "project": "abc"});
    let filter_match = serde_json::json!({"table": "orders", "project": "abc"});
    let filter_partial = serde_json::json!({"table": "orders", "project": "xyz"});
    assert!(EventStore::matches_filter(&payload, &filter_match));
    assert!(!EventStore::matches_filter(&payload, &filter_partial));
}

// ============================================================================
// Event type round trip
// ============================================================================

#[test]
fn event_type_round_trip() {
    use super::events::EventType;
    for et in [
        EventType::Cron,
        EventType::Manual,
        EventType::DataInsert,
        EventType::DataChange,
        EventType::PipelineCompleted,
    ] {
        assert_eq!(EventType::from_str(et.as_str()), Some(et));
    }
}

#[test]
fn event_type_from_unknown_returns_none() {
    use super::events::EventType;
    assert_eq!(EventType::from_str("unknown"), None);
}

// ============================================================================
// DAG validation with ReadStrategy variants
// ============================================================================

#[test]
fn dag_accepts_filter_strategy_source() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let nodes = vec![
        make_source_node(
            s,
            pid,
            ReadStrategy::Filter {
                table: "orders".into(),
                filter: "status = 'active'".into(),
            },
        ),
        make_transform_node(t, pid),
        make_sink_node(k, pid),
    ];
    let edges = vec![make_edge(pid, s, t), make_edge(pid, t, k)];

    let result = topological_sort(&nodes, &edges);
    assert!(result.is_ok());
}

#[test]
fn dag_accepts_batch_fetch_strategy_source() {
    let pid = Uuid::new_v4();
    let s = Uuid::new_v4();
    let t = Uuid::new_v4();
    let k = Uuid::new_v4();

    let nodes = vec![
        make_source_node(
            s,
            pid,
            ReadStrategy::BatchFetch {
                table: "orders".into(),
                batch_size: 500,
                max_rows: Some(10_000),
            },
        ),
        make_transform_node(t, pid),
        make_sink_node(k, pid),
    ];
    let edges = vec![make_edge(pid, s, t), make_edge(pid, t, k)];

    let result = topological_sort(&nodes, &edges);
    assert!(result.is_ok());
}

// ============================================================================
// NodeConfig serde round trip (source variant)
// ============================================================================

#[test]
fn node_config_source_serde_round_trip() {
    let config = NodeConfig::Source(SourceNodeConfig {
        connector_name: "pg".to_string(),
        read_strategy: ReadStrategy::CdcStream {
            table: "events".to_string(),
        },
    });

    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(json["type"], "source");
    assert_eq!(json["connector_name"], "pg");
    assert_eq!(json["read_strategy"]["strategy"], "cdc_stream");

    let deserialized: NodeConfig = serde_json::from_value(json).unwrap();
    match deserialized {
        NodeConfig::Source(src) => {
            assert_eq!(src.connector_name, "pg");
            assert!(matches!(src.read_strategy, ReadStrategy::CdcStream { .. }));
        }
        _ => panic!("expected Source variant"),
    }
}

#[test]
fn node_config_transform_serde_round_trip() {
    let config = NodeConfig::Transform(TransformNodeConfig {
        udf_name: "my_fn".to_string(),
        params: Default::default(),
    });
    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(json["type"], "transform");
    let deserialized: NodeConfig = serde_json::from_value(json).unwrap();
    assert!(matches!(deserialized, NodeConfig::Transform(_)));
}

#[test]
fn node_config_sink_serde_round_trip() {
    let config = NodeConfig::Sink(SinkNodeConfig {
        connector_name: "ch".to_string(),
        table: "out".to_string(),
    });
    let json = serde_json::to_value(&config).unwrap();
    assert_eq!(json["type"], "sink");
    let deserialized: NodeConfig = serde_json::from_value(json).unwrap();
    assert!(matches!(deserialized, NodeConfig::Sink(_)));
}

// ============================================================================
// Batch fetch max_rows optional
// ============================================================================

#[test]
fn batch_fetch_no_max_rows() {
    let strategy = ReadStrategy::BatchFetch {
        table: "t".to_string(),
        batch_size: 100,
        max_rows: None,
    };
    let json = serde_json::to_value(&strategy).unwrap();
    assert!(json.get("max_rows").is_none());

    let deserialized: ReadStrategy = serde_json::from_value(json).unwrap();
    assert_eq!(deserialized, strategy);
}
