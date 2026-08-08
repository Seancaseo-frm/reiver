//! Bidirectional mapping between Prometheus-style metric/label names and
//! OTEL semantic convention names.
//!
//! Used at two points in the Watch service:
//! - **Ingestion** (`metrics_worker`): map incoming Prometheus names to OTEL
//!   before storing in ClickHouse.
//! - **Query** (`promql_provider`, `widget_query`): map dashboard Prometheus
//!   names to OTEL for ClickHouse lookups.
//!
//! Metrics/labels already using OTEL names pass through unchanged.
//! Metrics/labels with no mapping entry pass through unchanged.

use std::collections::HashMap;

use once_cell::sync::Lazy;

// ---------------------------------------------------------------------------
// Metric name mapping
// ---------------------------------------------------------------------------

/// Prometheus PromQL name → OTEL storage name.
///
/// Only entries where the dashboard name differs from the stored name are
/// needed. Metrics already stored under their dashboard name (e.g.
/// `ClickHouseProfileEvents_*`) require no entry.
static METRIC_STORAGE_NAMES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    HashMap::from([
        // -- Kubernetes (native OTEL receivers) --------------------------------

        // kube-state-metrics → k8s_cluster receiver
        // Info metrics have no direct OTEL equivalent; map to closest
        // existing metric that carries the same resource labels (one
        // series per resource, used for counting/grouping in dashboards).
        ("kube_node_info", "k8s.node.condition_ready"),
        ("kube_pod_info", "k8s.pod.phase"),
        ("kube_pod_container_info", "k8s.container.ready"),
        // Allocatable resources: Prometheus uses one metric with a
        // `resource` label; OTEL emits separate metrics per resource.
        // We merge them into a single `k8s.node.allocatable` metric at
        // ingestion time, injecting `resource`/`unit` labels via
        // `synthetic_labels_for()`.
        ("kube_node_status_allocatable", "k8s.node.allocatable"),
        ("kube_pod_status_phase", "k8s.pod.phase"),
        ("kube_namespace_status_phase", "k8s.namespace.phase"),
        ("kube_node_status_condition", "k8s.node.condition_ready"),
        ("kube_deployment_spec_replicas", "k8s.deployment.desired"),
        (
            "kube_deployment_status_replicas_available",
            "k8s.deployment.available",
        ),
        (
            "kube_daemonset_status_current_number_scheduled",
            "k8s.daemonset.current_scheduled_nodes",
        ),
        (
            "kube_daemonset_status_desired_number_scheduled",
            "k8s.daemonset.desired_scheduled_nodes",
        ),
        (
            "kube_daemonset_status_number_ready",
            "k8s.daemonset.ready_nodes",
        ),
        ("kube_statefulset_replicas", "k8s.statefulset.desired_pods"),
        (
            "kube_statefulset_status_replicas_ready",
            "k8s.statefulset.ready_pods",
        ),
        (
            "kube_statefulset_status_replicas_current",
            "k8s.statefulset.current_pods",
        ),
        (
            "kube_statefulset_status_replicas_updated",
            "k8s.statefulset.updated_pods",
        ),
        (
            "kube_pod_container_status_restarts_total",
            "k8s.container.restarts",
        ),
        ("kube_replicaset_spec_replicas", "k8s.replicaset.desired"),
        (
            "kube_replicaset_status_ready_replicas",
            "k8s.replicaset.available",
        ),
        ("kube_job_spec_parallelism", "k8s.job.max_parallel_pods"),
        (
            "kube_job_spec_completions",
            "k8s.job.desired_successful_pods",
        ),
        ("kube_job_status_active", "k8s.job.active_pods"),
        ("kube_job_status_succeeded", "k8s.job.successful_pods"),
        ("kube_job_status_failed", "k8s.job.failed_pods"),
        (
            "kube_pod_container_resource_requests",
            "k8s.container.cpu_request",
        ),
        (
            "kube_pod_container_resource_limits",
            "k8s.container.cpu_limit",
        ),
        // cAdvisor → kubeletstats receiver (container level)
        ("container_cpu_usage_seconds_total", "container.cpu.time"),
        (
            "container_memory_working_set_bytes",
            "container.memory.working_set",
        ),
        ("container_memory_rss", "container.memory.rss"),
        ("container_memory_usage_bytes", "container.memory.usage"),
        ("container_fs_usage_bytes", "container.filesystem.usage"),
        ("container_fs_limit_bytes", "container.filesystem.capacity"),
        (
            "container_spec_memory_limit_bytes",
            "k8s.container.memory_limit",
        ),
        ("container_spec_cpu_quota", "k8s.container.cpu_limit"),
        (
            "container_network_receive_bytes_total",
            "k8s.pod.network.io",
        ),
        (
            "container_network_transmit_bytes_total",
            "k8s.pod.network.io",
        ),
        (
            "kubelet_volume_stats_available_bytes",
            "k8s.pod.filesystem.available",
        ),
        (
            "kubelet_volume_stats_used_bytes",
            "k8s.pod.filesystem.usage",
        ),
        // kubelet resource metrics (pod/node level)
        ("kubelet_pod_cpu_usage_seconds_total", "k8s.pod.cpu.time"),
        ("kubelet_pod_memory_usage_bytes", "k8s.pod.memory.usage"),
        ("kubelet_node_cpu_usage_seconds_total", "k8s.node.cpu.time"),
        ("kubelet_node_memory_usage_bytes", "k8s.node.memory.usage"),
        // node_exporter → hostmetrics receiver
        ("node_cpu_seconds_total", "system.cpu.time"),
        ("node_memory_MemTotal_bytes", "system.memory.usage"),
        ("node_memory_MemAvailable_bytes", "system.memory.usage"),
        ("node_memory_MemFree_bytes", "system.memory.usage"),
        ("node_filesystem_size_bytes", "system.filesystem.usage"),
        ("node_filesystem_size", "k8s.node.filesystem.capacity"),
        ("node_filesystem_avail_bytes", "system.filesystem.usage"),
        ("node_filesystem_free", "k8s.node.filesystem.available"),
        ("node_disk_read_bytes_total", "system.disk.io"),
        ("node_disk_written_bytes_total", "system.disk.io"),
        ("node_network_receive_bytes_total", "system.network.io"),
        ("node_network_transmit_bytes_total", "system.network.io"),
        ("node_load1", "system.cpu.load_average.1m"),
        ("node_load5", "system.cpu.load_average.5m"),
        ("node_load15", "system.cpu.load_average.15m"),
        // -- PostgreSQL (pg_* from postgres_exporter → OTEL postgresql receiver
        //    naming, see documentation.md) -------------------------------------
        ("pg_database_size_bytes", "postgresql.db_size"),
        ("pg_stat_database_xact_commit", "postgresql.commits"),
        ("pg_database_xact_commit", "postgresql.commits"),
        ("pg_stat_database_xact_rollback", "postgresql.rollbacks"),
        ("pg_database_xact_rollback", "postgresql.rollbacks"),
        ("pg_stat_activity_count", "postgresql.backends"),
        ("pg_settings_max_connections", "postgresql.connection.max"),
        ("pg_connections_max", "postgresql.connection.max"),
        ("pg_locks_count", "postgresql.database.locks"),
        ("pg_database_deadlocks", "postgresql.deadlocks"),
        ("pg_stat_database_deadlocks", "postgresql.deadlocks"),
        ("pg_database_blocks_hit", "postgresql.blocks_read"),
        ("pg_stat_database_blocks_hit", "postgresql.blocks_read"),
        ("pg_stat_database_blks_hit", "postgresql.blocks_read"),
        ("pg_database_blocks_read", "postgresql.blocks_read"),
        ("pg_stat_database_blocks_read", "postgresql.blocks_read"),
        ("pg_stat_database_blks_read", "postgresql.blocks_read"),
        ("pg_database_temp_bytes", "postgresql.temp.io"),
        ("pg_stat_database_temp_bytes", "postgresql.temp.io"),
        ("pg_database_temp_files", "postgresql.temp.files"),
        ("pg_stat_database_temp_files", "postgresql.temp.files"),
        ("pg_database_tup_inserted", "postgresql.tup_inserted"),
        ("pg_stat_database_tup_inserted", "postgresql.tup_inserted"),
        ("pg_database_tup_updated", "postgresql.tup_updated"),
        ("pg_stat_database_tup_updated", "postgresql.tup_updated"),
        ("pg_database_tup_deleted", "postgresql.tup_deleted"),
        ("pg_stat_database_tup_deleted", "postgresql.tup_deleted"),
        ("pg_database_tup_returned", "postgresql.tup_returned"),
        ("pg_stat_database_tup_returned", "postgresql.tup_returned"),
        ("pg_database_tup_fetched", "postgresql.tup_fetched"),
        ("pg_stat_database_tup_fetched", "postgresql.tup_fetched"),
        (
            "pg_bgwriter_buffers_alloc",
            "postgresql.bgwriter.buffers.allocated",
        ),
        (
            "pg_bgwriter_buffers_clean",
            "postgresql.bgwriter.buffers.writes",
        ),
        (
            "pg_bgwriter_buffers_backend",
            "postgresql.bgwriter.buffers.writes",
        ),
        (
            "pg_bgwriter_buffers_backend_fsync",
            "postgresql.bgwriter.buffers.writes",
        ),
        (
            "pg_bgwriter_buffers_checkpoint_clean",
            "postgresql.bgwriter.buffers.writes",
        ),
        (
            "pg_bgwriter_buffers_checkpoint_sync",
            "postgresql.bgwriter.buffers.writes",
        ),
        (
            "pg_bgwriter_checkpoints_timed",
            "postgresql.bgwriter.checkpoint.count",
        ),
        (
            "pg_bgwriter_checkpoints_scheduled",
            "postgresql.bgwriter.checkpoint.count",
        ),
        (
            "pg_replication_lag_bytes",
            "postgresql.replication.data_delay",
        ),
        (
            "pg_replication_slots_pg_wal_lsn_diff",
            "postgresql.replication.data_delay",
        ),
        ("pg_replication_lag_seconds", "postgresql.wal.lag"),
        ("pg_stat_user_tables_idx_scan", "postgresql.index.scans"),
        ("pg_stat_user_tables_n_live_tup", "postgresql.rows"),
        ("pg_stat_user_tables_n_dead_tup", "postgresql.rows"),
        (
            "pg_stat_user_tables_autovacuum_count",
            "postgresql.table.vacuum.count",
        ),
        ("pg_stat_user_tables_n_tup_ins", "postgresql.operations"),
        ("pg_stat_user_tables_n_tup_upd", "postgresql.operations"),
        ("pg_stat_user_tables_n_tup_del", "postgresql.operations"),
        (
            "pg_stat_user_tables_seq_scan",
            "postgresql.sequential_scans",
        ),
        ("pg_database_connection_limit", "postgresql.connection.max"),
        // -- Redpanda / Kafka (Kafka-protocol metrics → OTEL kafkametrics
        //    receiver naming) --------------------------------------------------
        //    Infrastructure metrics (cpu, memory, disk, rpc, raft) have no OTEL
        //    equivalent and pass through unchanged.
        (
            "redpanda_kafka_max_offset",
            "kafka.partition.current_offset",
        ),
        ("redpanda_kafka_partitions", "kafka.topic.partitions"),
        (
            "redpanda_kafka_under_replicated_replicas",
            "kafka.partition.replicas_in_sync",
        ),
        (
            "redpanda_kafka_consumer_group_lag",
            "kafka.consumer_group.lag",
        ),
        (
            "redpanda_kafka_consumer_group_committed_offset",
            "kafka.consumer_group.offset",
        ),
        (
            "redpanda_kafka_consumer_group_consumers",
            "kafka.consumer_group.members",
        ),
        ("redpanda_cluster_brokers", "kafka.brokers"),
        (
            "redpanda_kafka_replicas",
            "kafka.partition.replicas",
        ),
        (
            "redpanda_kafka_oldest_offset",
            "kafka.partition.oldest_offset",
        ),
        // -- Redis (redis_exporter Prometheus → OTEL redis receiver naming) ----
        ("redis_connected_clients", "redis.clients.connected"),
        ("redis_blocked_clients", "redis.clients.blocked"),
        ("redis_used_memory", "redis.memory.used"),
        ("redis_used_memory_rss", "redis.memory.rss"),
        ("redis_used_memory_peak", "redis.memory.peak"),
        ("redis_memory_used_bytes", "redis.memory.used"),
        ("redis_memory_max_bytes", "redis.maxmemory"),
        ("redis_keyspace_hits_total", "redis.keyspace.hits"),
        ("redis_keyspace_misses_total", "redis.keyspace.misses"),
        ("redis_commands_processed_total", "redis.commands.processed"),
        ("redis_instantaneous_ops_per_sec", "redis.commands"),
        ("redis_connections_received_total", "redis.connections.received"),
        ("redis_rejected_connections_total", "redis.connections.rejected"),
        ("redis_evicted_keys_total", "redis.keys.evicted"),
        ("redis_expired_keys_total", "redis.keys.expired"),
        ("redis_total_net_input_bytes", "redis.net.input"),
        ("redis_total_net_output_bytes", "redis.net.output"),
        ("redis_uptime_in_seconds", "redis.uptime"),
        ("redis_connected_slaves", "redis.slaves.connected"),
        (
            "redis_mem_fragmentation_ratio",
            "redis.memory.fragmentation_ratio",
        ),
        (
            "redis_rdb_changes_since_last_save",
            "redis.rdb.changes_since_last_save",
        ),
        ("redis_cpu_sys_seconds_total", "redis.cpu.time"),
        ("redis_cpu_user_seconds_total", "redis.cpu.time"),
        ("redis_latest_fork_usec", "redis.latest_fork"),
        ("redis_db_keys", "redis.db.keys"),
        ("redis_db_keys_expiring", "redis.db.expires"),
        ("redis_replication_offset", "redis.replication.offset"),
        (
            "redis_replication_backlog_first_byte_offset",
            "redis.replication.backlog_first_byte_offset",
        ),
        // -- k8s_cluster receiver allocatable metrics → merged storage name ----
        // The receiver emits separate metrics per resource type.  We merge
        // them into `k8s.node.allocatable` with synthetic labels (see
        // `synthetic_labels_for`).
        ("k8s.node.allocatable_cpu", "k8s.node.allocatable"),
        ("k8s.node.allocatable_memory", "k8s.node.allocatable"),
        ("k8s.node.allocatable_pods", "k8s.node.allocatable"),
        // -- HTTP semantic conventions (Prometheus naming → OTel storage) ------
        (
            "http_server_request_duration_seconds",
            "http.server.request.duration",
        ),
        (
            "http_server_requests_seconds",
            "http.server.request.duration",
        ),
        (
            "http_server_active_requests",
            "http.server.active_requests",
        ),
        (
            "http_client_request_duration_seconds",
            "http.client.request.duration",
        ),
        ("http_server_request_size_bytes", "http.server.request.body.size"),
        ("http_server_response_size_bytes", "http.server.response.body.size"),
        // -- ClickHouse --------------------------------------------------------
        // No OTEL equivalent. ClickHouseProfileEvents_* stays as-is.
    ])
});

/// Resolve the OTEL storage name for a Prometheus metric name.
///
/// Returns the mapped name if one exists, or `None` if the input name
/// should be used as-is (i.e. it is already the storage name).
#[inline]
pub fn resolve_storage_name(promql_name: &str) -> Option<&'static str> {
    METRIC_STORAGE_NAMES.get(promql_name).copied()
}

/// Normalize Prometheus-style histogram suffixes on OTel dotted metric
/// names to match what ingestion actually stores.
///
/// Ingestion (`metrics_worker`) expands histograms as:
///   `{base}.count`, `{base}.sum`, `{base}_bucket`
///
/// But users/templates may write the Prometheus convention:
///   `{base}_count`, `{base}_sum`
///
/// This function converts `_count` → `.count` and `_sum` → `.sum` when
/// the base name contains dots (indicating OTel naming). `_bucket` is
/// left untouched since ingestion already uses underscores for buckets.
///
/// Pure Prometheus names (no dots) are never modified.
pub fn normalize_histogram_suffix(name: &str) -> Option<String> {
    if let Some(base) = name.strip_suffix("_count") {
        if base.contains('.') {
            return Some(format!("{base}.count"));
        }
    }
    if let Some(base) = name.strip_suffix("_sum") {
        if base.contains('.') {
            return Some(format!("{base}.sum"));
        }
    }
    None
}

/// For Prometheus metrics that map to MULTIPLE OTEL metrics distinguished
/// by a label value (e.g. `kube_pod_container_resource_limits` →
/// `k8s.container.cpu_limit` OR `k8s.container.memory_limit` depending on
/// the `resource` label), return all variant storage names.
///
/// Each entry is `(storage_name, implicit_labels)`.  At query time the
/// provider should UNION data from all variants; the planner's label
/// filters will naturally select the correct subset.
///
/// Returns `None` for metrics that have a single canonical storage name.
#[inline]
pub fn resolve_storage_variants(
    promql_name: &str,
) -> Option<&'static [(&'static str, &'static [(&'static str, &'static str)])]> {
    match promql_name {
        "kube_pod_container_resource_limits" => Some(&[
            (
                "k8s.container.cpu_limit",
                &[("resource", "cpu"), ("unit", "core")],
            ),
            (
                "k8s.container.memory_limit",
                &[("resource", "memory"), ("unit", "byte")],
            ),
        ]),
        "kube_pod_container_resource_requests" => Some(&[
            (
                "k8s.container.cpu_request",
                &[("resource", "cpu"), ("unit", "core")],
            ),
            (
                "k8s.container.memory_request",
                &[("resource", "memory"), ("unit", "byte")],
            ),
        ]),
        // Old kube-state-metrics suffixed variants (pre-v2) that embed
        // the resource type in the metric name instead of a label.
        "kube_node_status_allocatable_pods" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "pods"), ("unit", "integer")] as &[_],
        )]),
        "kube_node_status_allocatable_cpu_cores" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "cpu"), ("unit", "core")],
        )]),
        "kube_node_status_allocatable_memory_bytes" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "memory"), ("unit", "byte")],
        )]),
        // No separate capacity metric in OTel; allocatable is the closest.
        "kube_node_status_capacity_pods" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "pods"), ("unit", "integer")],
        )]),
        "kube_node_status_capacity_cpu_cores" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "cpu"), ("unit", "core")],
        )]),
        "kube_node_status_capacity_memory_bytes" => Some(&[(
            "k8s.node.allocatable",
            &[("resource", "memory"), ("unit", "byte")],
        )]),
        // Old suffixed resource requests (pre-v2 kube-state-metrics)
        "kube_pod_container_resource_requests_cpu_cores" => Some(&[(
            "k8s.container.cpu_request",
            &[("resource", "cpu"), ("unit", "core")],
        )]),
        "kube_pod_container_resource_requests_memory_bytes" => Some(&[(
            "k8s.container.memory_request",
            &[("resource", "memory"), ("unit", "byte")],
        )]),
        _ => None,
    }
}

/// Return extra labels to inject when the incoming metric is merged into
/// a single storage metric.  Used at ingestion time to preserve the
/// Prometheus `resource`/`unit` semantics after OTEL's per-resource
/// metric split.
#[inline]
pub fn synthetic_labels_for(incoming_name: &str) -> &'static [(&'static str, &'static str)] {
    match incoming_name {
        "k8s.node.allocatable_cpu" => &[("resource", "cpu"), ("unit", "core")],
        "k8s.node.allocatable_memory" => &[("resource", "memory"), ("unit", "byte")],
        "k8s.node.allocatable_pods" => &[("resource", "pods"), ("unit", "integer")],
        // PostgreSQL: merged operation metrics
        "pg_stat_user_tables_n_tup_ins" => &[("operation", "ins")],
        "pg_stat_user_tables_n_tup_upd" => &[("operation", "upd")],
        "pg_stat_user_tables_n_tup_del" => &[("operation", "del")],
        // PostgreSQL: merged row state metrics
        "pg_stat_user_tables_n_live_tup" => &[("state", "live")],
        "pg_stat_user_tables_n_dead_tup" => &[("state", "dead")],
        // PostgreSQL: blocks_read source
        "pg_database_blocks_hit" | "pg_stat_database_blocks_hit" | "pg_stat_database_blks_hit" => {
            &[("source", "heap_hit")]
        }
        "pg_database_blocks_read" | "pg_stat_database_blocks_read" | "pg_stat_database_blks_read" => {
            &[("source", "heap_read")]
        }
        // PostgreSQL: bgwriter buffer sources
        "pg_bgwriter_buffers_clean" => &[("source", "bgwriter")],
        "pg_bgwriter_buffers_backend" => &[("source", "backend")],
        "pg_bgwriter_buffers_backend_fsync" => &[("source", "backend_fsync")],
        "pg_bgwriter_buffers_checkpoint_clean" => &[("source", "checkpoint")],
        "pg_bgwriter_buffers_checkpoint_sync" => &[("source", "checkpoint_sync")],
        // PostgreSQL: checkpoint type
        "pg_bgwriter_checkpoints_timed" => &[("type", "timed")],
        "pg_bgwriter_checkpoints_scheduled" => &[("type", "scheduled")],
        // Redis: CPU mode
        "redis_cpu_sys_seconds_total" => &[("state", "sys")],
        "redis_cpu_user_seconds_total" => &[("state", "user")],
        _ => &[],
    }
}

/// Return constant label values to inject at **query** time when a
/// Prometheus metric uses labels that don't exist in the OTEL data.
///
/// Covers two cases:
/// - Generic Prometheus metrics with a `resource` label mapping to
///   resource-specific OTEL metrics (e.g. `kube_pod_container_resource_limits`).
/// - cAdvisor metrics filtered by `device`/`id` labels that OTEL
///   container filesystem metrics don't carry.
#[inline]
pub fn implicit_query_labels(
    promql_name: &str,
    storage_name: &str,
) -> &'static [(&'static str, &'static str)] {
    match (promql_name, storage_name) {
        ("kube_pod_container_resource_limits", "k8s.container.cpu_limit") => {
            &[("resource", "cpu"), ("unit", "core")]
        }
        ("kube_pod_container_resource_requests", "k8s.container.cpu_request") => {
            &[("resource", "cpu"), ("unit", "core")]
        }
        // cAdvisor filesystem metrics filter on device=/dev/* and id=/
        // but OTEL container.filesystem.* metrics don't have these labels.
        ("container_fs_usage_bytes", "container.filesystem.usage") => {
            &[("device", "/dev/root"), ("id", "/")]
        }
        ("container_fs_limit_bytes", "container.filesystem.capacity") => {
            &[("device", "/dev/root"), ("id", "/")]
        }
        // Network I/O: receive and transmit share the same OTEL metric;
        // differentiate by direction attribute.
        ("container_network_receive_bytes_total", "k8s.pod.network.io") => {
            &[("direction", "receive")]
        }
        ("container_network_transmit_bytes_total", "k8s.pod.network.io") => {
            &[("direction", "transmit")]
        }
        ("node_network_receive_bytes_total", "system.network.io") => &[("direction", "receive")],
        ("node_network_transmit_bytes_total", "system.network.io") => &[("direction", "transmit")],
        // Disk I/O: read and write share the same OTEL metric.
        ("node_disk_read_bytes_total", "system.disk.io") => &[("direction", "read")],
        ("node_disk_written_bytes_total", "system.disk.io") => &[("direction", "write")],
        // PostgreSQL: blocks_read by source
        ("pg_database_blocks_hit", "postgresql.blocks_read") => &[("source", "heap_hit")],
        ("pg_stat_database_blocks_hit", "postgresql.blocks_read") => &[("source", "heap_hit")],
        ("pg_stat_database_blks_hit", "postgresql.blocks_read") => &[("source", "heap_hit")],
        ("pg_database_blocks_read", "postgresql.blocks_read") => &[("source", "heap_read")],
        ("pg_stat_database_blocks_read", "postgresql.blocks_read") => &[("source", "heap_read")],
        ("pg_stat_database_blks_read", "postgresql.blocks_read") => &[("source", "heap_read")],
        // PostgreSQL: operations by type
        ("pg_stat_user_tables_n_tup_ins", "postgresql.operations") => &[("operation", "ins")],
        ("pg_stat_user_tables_n_tup_upd", "postgresql.operations") => &[("operation", "upd")],
        ("pg_stat_user_tables_n_tup_del", "postgresql.operations") => &[("operation", "del")],
        // PostgreSQL: rows by state
        ("pg_stat_user_tables_n_live_tup", "postgresql.rows") => &[("state", "live")],
        ("pg_stat_user_tables_n_dead_tup", "postgresql.rows") => &[("state", "dead")],
        // Redis: CPU by mode
        ("redis_cpu_sys_seconds_total", "redis.cpu.time") => &[("state", "sys")],
        ("redis_cpu_user_seconds_total", "redis.cpu.time") => &[("state", "user")],
        _ => &[],
    }
}

/// Resolve implicit label filters for a query name and storage name.
///
/// Delegates to [`implicit_query_labels`] for Prometheus-compat names. OTEL-native
/// queries (`query_name == storage_name`) return no implicit filters — callers
/// should use explicit equality matchers (e.g. `direction="receive"`).
#[inline]
pub fn implicit_query_labels_for(
    query_name: &str,
    storage_name: &str,
) -> &'static [(&'static str, &'static str)] {
    implicit_query_labels(query_name, storage_name)
}

// ---------------------------------------------------------------------------
// Label name mapping
// ---------------------------------------------------------------------------

/// Prometheus label name → OTEL attribute name.
static LABEL_STORAGE_NAMES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    HashMap::from([
        // K8s
        ("namespace", "k8s.namespace.name"),
        ("pod", "k8s.pod.name"),
        ("node", "k8s.node.name"),
        ("container", "k8s.container.name"),
        ("deployment", "k8s.deployment.name"),
        ("daemonset", "k8s.daemonset.name"),
        ("statefulset", "k8s.statefulset.name"),
        ("replicaset", "k8s.replicaset.name"),
        ("uid", "k8s.pod.uid"),
        // Common infrastructure
        ("instance", "service.instance.id"),
        // OTel resource attributes (underscore form → dotted storage form)
        ("service_name", "service.name"),
        ("service_version", "service.version"),
        ("service_namespace", "service.namespace"),
        ("service_instance_id", "service.instance.id"),
        ("deployment_environment", "deployment.environment"),
        ("telemetry_sdk_name", "telemetry.sdk.name"),
        ("telemetry_sdk_language", "telemetry.sdk.language"),
        ("telemetry_sdk_version", "telemetry.sdk.version"),
        // HTTP semantic conventions
        ("http_route", "http.route"),
        ("http_method", "http.request.method"),
        ("http_request_method", "http.request.method"),
        ("http_status_code", "http.response.status_code"),
        ("http_response_status_code", "http.response.status_code"),
        ("http_target", "url.path"),
        ("http_scheme", "url.scheme"),
        ("http_url", "url.full"),
        ("url_path", "url.path"),
        ("url_scheme", "url.scheme"),
        ("url_full", "url.full"),
        // Network / Server
        ("net_host_name", "server.address"),
        ("net_host_port", "server.port"),
        ("server_address", "server.address"),
        ("server_port", "server.port"),
        ("net_peer_name", "network.peer.address"),
        ("net_peer_port", "network.peer.port"),
        ("network_peer_address", "network.peer.address"),
        ("network_peer_port", "network.peer.port"),
        // RPC
        ("rpc_service", "rpc.service"),
        ("rpc_method", "rpc.method"),
        ("rpc_system", "rpc.system"),
        // Database
        ("db_system", "db.system"),
        ("db_name", "db.name"),
        ("db_operation", "db.operation"),
        ("db_statement", "db.statement"),
        // Messaging
        ("messaging_system", "messaging.system"),
        ("messaging_destination", "messaging.destination.name"),
        ("messaging_operation", "messaging.operation"),
        // PostgreSQL
        ("datname", "postgresql.database.name"),
        ("relname", "postgresql.table.name"),
        ("schemaname", "postgresql.schema.name"),
        // Kafka / Redpanda
        ("topic", "kafka.topic"),
        ("redpanda_topic", "kafka.topic"),
        ("group", "kafka.consumer_group"),
        ("redpanda_group", "kafka.consumer_group"),
        ("partition", "kafka.partition"),
        ("redpanda_partition", "kafka.partition"),
    ])
});

/// Resolve the OTEL attribute name for a Prometheus label name.
///
/// Returns the mapped name if one exists, or `None` if the input label
/// should be used as-is.
#[inline]
pub fn resolve_label_name(promql_label: &str) -> Option<&'static str> {
    LABEL_STORAGE_NAMES.get(promql_label).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_kube_state_metrics() {
        assert_eq!(
            resolve_storage_name("kube_pod_status_phase"),
            Some("k8s.pod.phase")
        );
        assert_eq!(
            resolve_storage_name("kube_deployment_spec_replicas"),
            Some("k8s.deployment.desired")
        );
        assert_eq!(
            resolve_storage_name("kube_node_status_condition"),
            Some("k8s.node.condition_ready")
        );
        assert_eq!(
            resolve_storage_name("kube_statefulset_replicas"),
            Some("k8s.statefulset.desired_pods")
        );
        assert_eq!(
            resolve_storage_name("kube_node_info"),
            Some("k8s.node.condition_ready")
        );
        assert_eq!(resolve_storage_name("kube_pod_info"), Some("k8s.pod.phase"));
        assert_eq!(
            resolve_storage_name("kube_pod_container_info"),
            Some("k8s.container.ready")
        );
        assert_eq!(
            resolve_storage_name("kube_node_status_allocatable"),
            Some("k8s.node.allocatable")
        );
    }

    #[test]
    fn resolves_allocatable_ingestion_merge() {
        assert_eq!(
            resolve_storage_name("k8s.node.allocatable_cpu"),
            Some("k8s.node.allocatable")
        );
        assert_eq!(
            resolve_storage_name("k8s.node.allocatable_memory"),
            Some("k8s.node.allocatable")
        );
    }

    #[test]
    fn synthetic_labels_for_allocatable() {
        assert_eq!(
            synthetic_labels_for("k8s.node.allocatable_cpu"),
            &[("resource", "cpu"), ("unit", "core")]
        );
        assert_eq!(
            synthetic_labels_for("k8s.node.allocatable_memory"),
            &[("resource", "memory"), ("unit", "byte")]
        );
        assert!(synthetic_labels_for("container.cpu.time").is_empty());
    }

    #[test]
    fn synthetic_labels_for_postgres() {
        assert_eq!(
            synthetic_labels_for("pg_stat_user_tables_n_tup_ins"),
            &[("operation", "ins")]
        );
        assert_eq!(
            synthetic_labels_for("pg_stat_user_tables_n_tup_upd"),
            &[("operation", "upd")]
        );
        assert_eq!(
            synthetic_labels_for("pg_stat_user_tables_n_tup_del"),
            &[("operation", "del")]
        );
        assert_eq!(
            synthetic_labels_for("pg_stat_user_tables_n_live_tup"),
            &[("state", "live")]
        );
        assert_eq!(
            synthetic_labels_for("pg_stat_user_tables_n_dead_tup"),
            &[("state", "dead")]
        );
    }

    #[test]
    fn resolves_cadvisor_and_kubelet() {
        assert_eq!(
            resolve_storage_name("container_cpu_usage_seconds_total"),
            Some("container.cpu.time")
        );
        assert_eq!(
            resolve_storage_name("container_memory_working_set_bytes"),
            Some("container.memory.working_set")
        );
        assert_eq!(
            resolve_storage_name("kubelet_pod_cpu_usage_seconds_total"),
            Some("k8s.pod.cpu.time")
        );
    }

    #[test]
    fn resolves_node_exporter() {
        assert_eq!(
            resolve_storage_name("node_cpu_seconds_total"),
            Some("system.cpu.time")
        );
        assert_eq!(
            resolve_storage_name("node_load1"),
            Some("system.cpu.load_average.1m")
        );
        assert_eq!(
            resolve_storage_name("node_disk_read_bytes_total"),
            Some("system.disk.io")
        );
    }

    #[test]
    fn resolves_postgres() {
        assert_eq!(
            resolve_storage_name("pg_database_size_bytes"),
            Some("postgresql.db_size")
        );
        assert_eq!(
            resolve_storage_name("pg_stat_database_xact_commit"),
            Some("postgresql.commits")
        );
        assert_eq!(
            resolve_storage_name("pg_stat_activity_count"),
            Some("postgresql.backends")
        );
        assert_eq!(
            resolve_storage_name("pg_locks_count"),
            Some("postgresql.database.locks")
        );
        assert_eq!(
            resolve_storage_name("pg_bgwriter_buffers_alloc"),
            Some("postgresql.bgwriter.buffers.allocated")
        );
        assert_eq!(
            resolve_storage_name("pg_replication_lag_bytes"),
            Some("postgresql.replication.data_delay")
        );
        assert_eq!(
            resolve_storage_name("pg_database_blocks_hit"),
            Some("postgresql.blocks_read")
        );
        assert_eq!(
            resolve_storage_name("pg_database_blocks_read"),
            Some("postgresql.blocks_read")
        );
    }

    #[test]
    fn resolves_redpanda_kafka() {
        assert_eq!(
            resolve_storage_name("redpanda_kafka_max_offset"),
            Some("kafka.partition.current_offset")
        );
        assert_eq!(
            resolve_storage_name("redpanda_kafka_partitions"),
            Some("kafka.topic.partitions")
        );
        assert_eq!(
            resolve_storage_name("redpanda_kafka_consumer_group_lag"),
            Some("kafka.consumer_group.lag")
        );
        assert_eq!(
            resolve_storage_name("redpanda_kafka_consumer_group_committed_offset"),
            Some("kafka.consumer_group.offset")
        );
        assert_eq!(
            resolve_storage_name("redpanda_cluster_brokers"),
            Some("kafka.brokers")
        );
    }

    #[test]
    fn resolves_redis() {
        assert_eq!(
            resolve_storage_name("redis_connected_clients"),
            Some("redis.clients.connected")
        );
        assert_eq!(
            resolve_storage_name("redis_used_memory"),
            Some("redis.memory.used")
        );
        assert_eq!(
            resolve_storage_name("redis_keyspace_hits_total"),
            Some("redis.keyspace.hits")
        );
        assert_eq!(
            resolve_storage_name("redis_rejected_connections_total"),
            Some("redis.connections.rejected")
        );
        assert_eq!(
            resolve_storage_name("redis_evicted_keys_total"),
            Some("redis.keys.evicted")
        );
    }

    #[test]
    fn resolves_http_semantic_convention_metrics() {
        assert_eq!(
            resolve_storage_name("http_server_request_duration_seconds"),
            Some("http.server.request.duration")
        );
        assert_eq!(
            resolve_storage_name("http_server_requests_seconds"),
            Some("http.server.request.duration")
        );
        assert_eq!(
            resolve_storage_name("http_client_request_duration_seconds"),
            Some("http.client.request.duration")
        );
    }

    #[test]
    fn unmapped_returns_none() {
        assert_eq!(
            resolve_storage_name("ClickHouseProfileEvents_InsertedRows"),
            None
        );
        assert_eq!(
            resolve_storage_name("redpanda_cpu_busy_seconds_total"),
            None
        );
        assert_eq!(resolve_storage_name("pg_up"), None);
    }

    #[test]
    fn resolves_k8s_labels() {
        assert_eq!(resolve_label_name("namespace"), Some("k8s.namespace.name"));
        assert_eq!(resolve_label_name("pod"), Some("k8s.pod.name"));
        assert_eq!(resolve_label_name("node"), Some("k8s.node.name"));
        assert_eq!(resolve_label_name("container"), Some("k8s.container.name"));
    }

    #[test]
    fn resolves_postgres_labels() {
        assert_eq!(
            resolve_label_name("datname"),
            Some("postgresql.database.name")
        );
        assert_eq!(resolve_label_name("relname"), Some("postgresql.table.name"));
    }

    #[test]
    fn resolves_redpanda_labels() {
        assert_eq!(resolve_label_name("redpanda_topic"), Some("kafka.topic"));
        assert_eq!(
            resolve_label_name("redpanda_group"),
            Some("kafka.consumer_group")
        );
        assert_eq!(
            resolve_label_name("redpanda_partition"),
            Some("kafka.partition")
        );
    }

    #[test]
    fn resolves_otel_resource_labels() {
        assert_eq!(resolve_label_name("service_name"), Some("service.name"));
        assert_eq!(resolve_label_name("service_version"), Some("service.version"));
        assert_eq!(
            resolve_label_name("service_namespace"),
            Some("service.namespace")
        );
        assert_eq!(
            resolve_label_name("deployment_environment"),
            Some("deployment.environment")
        );
    }

    #[test]
    fn resolves_http_labels() {
        assert_eq!(resolve_label_name("http_route"), Some("http.route"));
        assert_eq!(
            resolve_label_name("http_method"),
            Some("http.request.method")
        );
        assert_eq!(
            resolve_label_name("http_status_code"),
            Some("http.response.status_code")
        );
        assert_eq!(
            resolve_label_name("http_response_status_code"),
            Some("http.response.status_code")
        );
    }

    #[test]
    fn unmapped_label_returns_none() {
        assert_eq!(resolve_label_name("le"), None);
        assert_eq!(resolve_label_name("mode"), None);
        assert_eq!(resolve_label_name("k8s.namespace.name"), None);
    }

    #[test]
    fn normalize_histogram_suffix_otel_names() {
        assert_eq!(
            normalize_histogram_suffix("http.server.request.duration_count"),
            Some("http.server.request.duration.count".to_string())
        );
        assert_eq!(
            normalize_histogram_suffix("http.server.request.duration_sum"),
            Some("http.server.request.duration.sum".to_string())
        );
        // _bucket is already correct — no normalization needed
        assert_eq!(
            normalize_histogram_suffix("http.server.request.duration_bucket"),
            None
        );
        // Already correct dot form — no change
        assert_eq!(
            normalize_histogram_suffix("http.server.request.duration.count"),
            None
        );
        // Pure Prometheus names (no dots) — never modified
        assert_eq!(
            normalize_histogram_suffix("http_request_duration_seconds_count"),
            None
        );
        assert_eq!(
            normalize_histogram_suffix("pg_stat_activity_count"),
            None
        );
    }
}
