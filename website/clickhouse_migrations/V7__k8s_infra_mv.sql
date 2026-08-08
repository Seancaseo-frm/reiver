-- Pre-aggregated k8s infrastructure metrics with extracted resource attributes.
-- Eliminates Map column access at query time and reduces row count via 1-minute bucketing.

CREATE TABLE IF NOT EXISTS reiver.k8s_infra_1m_local (
    project_id          UUID,
    metric_name         LowCardinality(String),
    pod_name            String                    DEFAULT '',
    node_name           String                    DEFAULT '',
    container_name      String                    DEFAULT '',
    deployment_name     String                    DEFAULT '',
    namespace           LowCardinality(String)    DEFAULT '',
    cluster             LowCardinality(String)    DEFAULT '',
    cpu_id              String                    DEFAULT '',
    memory_state        LowCardinality(String)    DEFAULT '',
    unix_milli          Int64,
    value_sum           SimpleAggregateFunction(sum, Float64),
    value_count         SimpleAggregateFunction(sum, UInt64),
    value_min           SimpleAggregateFunction(min, Float64),
    value_max           SimpleAggregateFunction(max, Float64)
)
ENGINE = ReplicatedAggregatingMergeTree()
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (project_id, metric_name, pod_name, node_name, container_name,
          deployment_name, namespace, cluster, cpu_id, memory_state, unix_milli)
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalDay(30)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS reiver.k8s_infra_1m (
    project_id          UUID,
    metric_name         LowCardinality(String),
    pod_name            String                    DEFAULT '',
    node_name           String                    DEFAULT '',
    container_name      String                    DEFAULT '',
    deployment_name     String                    DEFAULT '',
    namespace           LowCardinality(String)    DEFAULT '',
    cluster             LowCardinality(String)    DEFAULT '',
    cpu_id              String                    DEFAULT '',
    memory_state        LowCardinality(String)    DEFAULT '',
    unix_milli          Int64,
    value_sum           SimpleAggregateFunction(sum, Float64),
    value_count         SimpleAggregateFunction(sum, UInt64),
    value_min           SimpleAggregateFunction(min, Float64),
    value_max           SimpleAggregateFunction(max, Float64)
)
ENGINE = Distributed('{cluster}', 'reiver', 'k8s_infra_1m_local',
                     cityHash64(project_id));

CREATE MATERIALIZED VIEW IF NOT EXISTS reiver.k8s_infra_1m_mv
TO reiver.k8s_infra_1m_local
AS SELECT
    project_id,
    metric_name,
    resource_attributes['k8s.pod.name']        AS pod_name,
    resource_attributes['k8s.node.name']       AS node_name,
    resource_attributes['k8s.container.name']  AS container_name,
    resource_attributes['k8s.deployment.name'] AS deployment_name,
    resource_attributes['k8s.namespace.name']  AS namespace,
    resource_attributes['k8s.cluster.name']    AS cluster,
    metric_attributes['cpu']                   AS cpu_id,
    metric_attributes['state']                 AS memory_state,
    intDiv(unix_milli, 60000) * 60000          AS unix_milli,
    value                                      AS value_sum,
    toUInt64(1)                                AS value_count,
    value                                      AS value_min,
    value                                      AS value_max
FROM reiver.samples_v1_local
WHERE metric_name IN (
    'k8s.pod.cpu.usage', 'k8s.pod.cpu.utilization',
    'k8s.pod.memory.usage', 'k8s.pod.memory.working_set',
    'k8s.container.restarts',
    'container.cpu.usage', 'container.memory.usage',
    'k8s.node.cpu.usage',
    'k8s.node.memory.usage', 'k8s.node.memory.available',
    'k8s.node.memory.working_set',
    'k8s.node.filesystem.capacity', 'k8s.node.filesystem.usage',
    'k8s.deployment.desired',
    'system.cpu.time', 'system.memory.usage',
    'system.filesystem.usage_total', 'system.filesystem.usage_used'
);
