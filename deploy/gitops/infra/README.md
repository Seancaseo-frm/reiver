# GitOps infra: operators and data services

All resources deploy into namespace **reiver-infra**. The Reiver app (namespace **reiver**) connects via in-cluster DNS.

## Components

| Component | Chart / CR | Service (in-cluster) | App env |
|-----------|------------|----------------------|----------|
| **PostgreSQL** | CloudNativePG operator + Cluster CR | `postgres-cluster-rw.reiver-infra.svc:5432` | `DATABASE_URL` |
| **ClickHouse** | ClickHouse operator + Cluster CR | `clickhouse-clickhouse.reiver-infra.svc:8123` | `CLICKHOUSE_URL` |
| **Redis** | Bitnami Redis | `redis-master.reiver-infra.svc:6379` | `REDIS_URL` |
| **Redpanda** | Redpanda Helm | `redpanda.reiver-infra.svc:9092` (or 9093; check chart defaults) | `KAFKA_HOSTS` |
| **Jaeger** | Plain manifests (all-in-one) | `jaeger.reiver-infra.svc:4318` (OTLP), `:16686` (UI) | `OTEL_EXPORTER_OTLP_ENDPOINT` |

## Postgres: app user and credentials

The CloudNativePG **Cluster** creates the database `reiver` and a **superuser** secret: `postgres-cluster-superuser` (username/password). You can:

- Use the superuser in app Secrets (quick for dev), or
- Create a dedicated app user: connect as superuser and run `CREATE USER app WITH PASSWORD '...'; GRANT ALL ON DATABASE reiver TO app;` then use `app` in `DATABASE_URL`.

See [deploy/k8s/SECRETS.md](../../k8s/SECRETS.md) for in-cluster connection string examples.

## Adding or upgrading a component

- **Postgres**: Edit [postgres/cluster.yaml](postgres/cluster.yaml) or [postgres/kustomization.yaml](postgres/kustomization.yaml) (operator version in `helmCharts`).
- **ClickHouse / Redis / Redpanda**: Edit the component's `kustomization.yaml` (`helmCharts` version and `valuesInline`). Then commit and push; Argo CD will sync.
- **Jaeger**: Edit [jaeger/jaeger.yaml](jaeger/jaeger.yaml). Uses in-memory storage by default; no external DB needed.

## Jaeger (trace viewer)

Watch and watch-workers export OTLP traces to Jaeger for ingestion pipeline observability. Access the UI:

```bash
make jaeger   # http://localhost:16686
```

---

## Scaling

### Node roles and labels

Workload placement is controlled by the `reiver.io/role` node label:

| Label value | Purpose | Example workloads |
|-------------|---------|-------------------|
| `ci` | CI/CD builds | Drone server, runner, build pods |
| *(none)* | General worker | App pods (flow, watch, website, herd, mcp) |
| `storage` | Dedicated data nodes | ClickHouse, Postgres, Redis, Redpanda |

The primary (control-plane) node is labeled `reiver.io/role=ci`. Worker nodes without a label accept all workloads except CI. To create a dedicated storage node, label it and add a taint so only data services schedule there:

```bash
kubectl label node <node> reiver.io/role=storage
kubectl taint node <node> reiver.io/role=storage:NoSchedule
```

Then add a matching `tolerations` and `nodeSelector` to the infra component configs (see per-component sections below).

### Adding a node

```bash
./deploy/scripts/setup-server.sh add-node <IP> [user]
```

The script installs k3s in agent mode and joins the node to the cluster. Label the node after it joins.

### Per-component scaling

#### Postgres

| File | Field | Default | Effect |
|------|-------|---------|--------|
| `postgres/cluster.yaml` | `spec.instances` | 2 | Number of Postgres instances (1 primary + N-1 streaming replicas) |

CloudNativePG handles replication, failover, and promotion automatically. The `-rw` service always points to the current primary. Replicas serve read traffic via `-ro`.

Scaling up: increment `instances`, commit, push. The operator creates a new replica, streams a base backup, and starts WAL replication. No app changes needed.

#### Redpanda (Kafka)

| File | Field | Default | Effect |
|------|-------|---------|--------|
| `redpanda/kustomization.yaml` | `statefulset.replicas` | 1 | Number of Redpanda brokers |

Scaling up: increment `replicas`, commit, push. The new broker joins the cluster and partition leaders rebalance automatically. The service `redpanda.reiver-infra.svc:9092` resolves to all brokers.

With 2 brokers, keep `replication.factor=1` on topics (see "2-node quorum limitations" below). At 3+ brokers, set `replication.factor=2` for data redundancy.

#### Redis

| File | Field | Default | Effect |
|------|-------|---------|--------|
| `redis/kustomization.yaml` | `architecture` | `standalone` | `standalone` or `replication` |
| `redis/kustomization.yaml` | `replica.replicaCount` | 0 | Number of read replicas |

For HA: change `architecture` to `replication` and set `replica.replicaCount` to 1+. Read replicas serve via `redis-replicas.reiver-infra.svc:6379`. Writes still go to `redis-master`.

For most caching/dedup workloads, standalone is sufficient. Scale only if you need HA or read throughput.

#### ClickHouse

| File | Field | Current | Effect |
|------|-------|---------|--------|
| `clickhouse/cluster.yaml` | `spec.shards` | 3 | Number of shards (data distribution units) |
| `clickhouse/cluster.yaml` | `spec.replicas` | 1 | Number of replicas per shard |
| `clickhouse/keeper.yaml` | `spec.replicas` | 1 | Number of ClickHouse Keeper (consensus) instances |

**Architecture:** Local storage tables use a `_local` suffix (`spans_local`, `llm_requests_local`, etc.) with `ReplicatedMergeTree` engines. Bare-name tables (`spans`, `llm_requests`, etc.) are `Distributed` tables that route queries/inserts across all shards. Sharding key is `cityHash64(project_id)` — all data for a given project lives on the same shard.

**Server setting:** `distributed_product_mode = 'local'` is set via a ClickHouse settings profile (`reiver_default`). This allows subqueries that reference multiple Distributed tables (e.g., PromQL widget queries joining `samples_v1` with `time_series_v1`) to work correctly by rewriting inner subqueries to use `_local` tables. This is safe because all tables use the same sharding key, guaranteeing data co-locality.

**Scaling up (adding a shard):** increment `spec.shards` in `cluster.yaml`, commit, push. The operator creates a new shard pod. The `Replicated` database engine propagates all DDL (table schemas) to the new shard automatically. New inserts are distributed across all shards by the `Distributed` tables. Existing data stays on the original shards — queries still work correctly because `Distributed` fans out to all shards.

Keeper stays at 1 replica until 3+ nodes are available (see "2-node quorum limitations" below).

For dedicated storage nodes, add `nodeSelector` and `tolerations` to both `cluster.yaml` and `keeper.yaml` so ClickHouse pods schedule only on storage-labeled nodes.

ClickHouse is the largest consumer of disk (150GB+). Isolating it on dedicated storage nodes with large disks is recommended for production.

##### Tiered storage (hot NVMe + cold R2)

All `_local` tables with significant data volume use `storage_policy = 'tiered'`. This policy defines two volumes:

- **Hot (default disk):** Local NVMe on the ClickHouse node. All inserts land here. Fast queries.
- **Cold (r2_cold disk):** Cloudflare R2 (S3-compatible). ClickHouse moves older parts here automatically.

Tiering is **size-based**, not time-based. The `move_factor = 0.2` setting means ClickHouse starts moving the oldest parts to R2 when free space drops below 20% of total disk (i.e., disk is ~80% full). If the disk never fills up, everything stays local.

**Configuration:**
- Storage policy: embedded in `clickhouse/cluster.yaml` via `spec.settings.extraConfig.storage_configuration`
- R2 credentials: `clickhouse-r2-credentials` secret in `reiver-infra` (see [SECRETS.md](../../k8s/SECRETS.md)), injected as env vars via `containerTemplate.env` and read by ClickHouse's `@from_env` directives

**Monitoring:**

```sql
-- Check which disks ClickHouse sees
SELECT name, path, free_space, total_space FROM system.disks;

-- Check storage policies
SELECT policy_name, volume_name, disks FROM system.storage_policies;

-- Check data distribution across disks
SELECT disk_name, formatReadableSize(sum(bytes_on_disk)) AS size, count() AS parts
FROM system.parts WHERE database = 'reiver' AND active = 1
GROUP BY disk_name;
```

**Tuning:** Adjust `move_factor` in `cluster.yaml` under `spec.settings.extraConfig.storage_configuration.policies.tiered`. The value represents the free-space threshold: `0.2` means "move when free space < 20%" (disk ~80% full). Lower values keep more data local. R2 has zero egress fees, so querying cold data has no transfer cost — only storage ($0.015/GB/month).

Small reference tables (`otlp_attributes_local`, `discovered_services_local`, etc.) do not use the tiered policy and stay on local disk.

##### Rebalancing data across shards

ClickHouse does **not** automatically rebalance data when shards are added. New inserts follow the sharding key hash to all shards, but historical data stays where it was written. If a shard runs low on disk (e.g., a high-volume project fills its shard), move older partitions to another shard.

All tables are partitioned by `toYYYYMM(timestamp)`, so each partition is one calendar month.

**Step 1 — Identify what to move**

Check partition sizes on the overloaded shard:

```sql
SELECT
    table,
    partition,
    partition_id,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE database = 'reiver' AND active = 1
GROUP BY table, partition, partition_id
ORDER BY sum(bytes_on_disk) DESC;
```

Pick the table and partition to move (e.g., `spans_local` partition `202605`).

**Step 2 — Find the source shard's Keeper path**

On the **source shard** (the one with the data):

```sql
SELECT zookeeper_path FROM system.replicas
WHERE database = 'reiver' AND table = 'spans_local';
```

This returns a path like `/clickhouse/tables/<uuid>/<shard_num>`. The `<shard_num>` suffix identifies the shard (0, 1, 2, ...). You need the source shard's full path. (The column is named `zookeeper_path` for historical reasons — it points to ClickHouse Keeper, not ZooKeeper.)

**Step 3 — Fetch the partition on the destination shard**

Connect to the **destination shard** (the one you want to move data to) and run:

```sql
ALTER TABLE reiver.spans_local
    FETCH PARTITION '202605'
    FROM '/clickhouse/tables/<uuid>/<source_shard_num>';
```

This uses ClickHouse Keeper to locate the source shard's replica, then downloads the partition data over the network into the destination's `detached/` directory. No data is served yet. The system verifies that the table structure matches before downloading.

Note: `FETCH PARTITION` is **not replicated** — it only places data in the `detached/` directory on the node you run it on.

**Step 4 — Attach the partition on the destination shard**

Still on the **destination shard**:

```sql
ALTER TABLE reiver.spans_local
    ATTACH PARTITION '202605';
```

This makes the data active and queryable. `ATTACH PARTITION` **is replicated** — if the destination shard has replicas, the data propagates to them automatically.

**Step 5 — Verify before dropping the source**

Query the Distributed table to confirm the data is visible from both shards:

```sql
SELECT count() FROM reiver.spans WHERE toYYYYMM(timestamp) = 202605;
```

Also verify directly on the destination shard:

```sql
-- On the destination shard
SELECT count() FROM reiver.spans_local WHERE toYYYYMM(timestamp) = 202605;
```

**Step 6 — Drop the partition from the source shard**

Only after confirming the data exists on the destination, drop it from the **source shard**:

```sql
ALTER TABLE reiver.spans_local
    DROP PARTITION '202605';
```

This is replicated — if the source shard has replicas, the partition is dropped from all of them.

**Important considerations:**

- **No downtime.** Reads continue via the Distributed table throughout the process. During steps 3-4, the partition exists on both shards temporarily (the Distributed table deduplicates by fan-out, and queries will not double-count since `FETCH` copies to `detached/` which is invisible until `ATTACH`).
- **Between ATTACH and DROP** the partition exists on both shards. This is fine — the Distributed table fans out to all shards, so queries see both copies. The data is identical, but aggregation queries (COUNT, SUM) will double-count during this window. Keep it short.
- **Repeat per table.** Each `_local` table must be moved independently. If you move a month of `spans_local`, also move the same month of `logs_local`, `samples_v1_local`, etc. from the same shard to keep data co-located by project. Tables with MaterializedViews (e.g., `llm_requests_local` feeds `llm_cost_daily_local`) should have both the source and target MV tables moved together.
- **Check `system.detached_parts`** on the destination before starting. Old detached parts from previous operations could get accidentally attached. Clean them first with `ALTER TABLE ... DROP DETACHED PARTITION ... SETTINGS allow_drop_detached = 1`.
- **Partition IDs** use the format from `system.parts.partition_id`. For `toYYYYMM` partitioning, the partition ID is the 6-digit year-month string (e.g., `202605`).
- **Large partitions** take time to transfer (network-bound). Monitor progress via `system.part_log` on the destination. For very large partitions, consider moving individual parts instead of whole partitions using `FETCH PART` / `ATTACH PART`.
- **This procedure does not change where new inserts go.** The sharding key hash determines insert routing. After moving a partition, new data for the same project still goes to the original shard. This procedure is for freeing disk space, not changing the sharding topology.

##### Scaling strategy for large tenants

The current architecture (3 shards, hash-based routing via `cityHash64(project_id)`, tiered storage with R2 cold tier) is designed for many small-to-medium tenants. Each tenant's data lives on one shard, TTLs keep storage bounded, and the cold tier absorbs overflow if a shard fills up. No manual intervention needed for normal operation.

When a single tenant grows large enough to strain a shard, two strategies are viable — **do not duplicate table schemas**.

**Strategy 1: Dedicated shards with application-level routing**

Add shards with larger disks to the existing cluster and route the heavy tenant there explicitly:

1. Add a new shard (`spec.shards` in `cluster.yaml`) backed by a storage-heavy node
2. The `Replicated` database engine propagates all DDL to the new shard automatically
3. Move the tenant's existing data using the FETCH/ATTACH/DROP procedure (see "Rebalancing data across shards" above)
4. Add a routing override in Postgres: a `project_shard_overrides` table mapping `project_id → shard_num`
5. Modify the ingestion code to check this table — if an override exists, insert directly to that shard's `_local` table instead of going through the Distributed table
6. Queries still work transparently: Distributed tables fan out to all shards, so the tenant's data is found regardless of which shard it's on

This keeps a single set of table definitions, a single cluster, and a single query path. The only application change is shard-aware inserts for overridden tenants.

**Strategy 2: Separate cluster for whale tenants**

For tenants that are orders of magnitude larger, run a dedicated ClickHouse cluster:

1. Deploy a second `ClickHouseCluster` CR (e.g., `clickhouse-heavy`) with its own Keeper, larger disks, and more shards
2. Run the same migration against it (the SQL uses `IF NOT EXISTS` and is fully idempotent)
3. Route the tenant's ingestion and queries to the heavy cluster at the application level (separate `CLICKHOUSE_URL` per tenant tier)
4. The heavy cluster can have its own storage policy tuning (lower `move_factor`, larger PVCs, different TTLs)

This provides complete isolation — the whale tenant can't impact other tenants' query performance. The trade-off is operational complexity: two clusters to manage, two sets of credentials, and application-level routing logic.

**When to use which:**

| Signal | Strategy |
|--------|----------|
| A tenant fills a shard but cluster has capacity | Strategy 1 (dedicated shard) |
| A tenant's query load impacts other tenants | Strategy 2 (separate cluster) |
| You have 3+ whale tenants | Strategy 2 (separate cluster) |
| You want to offer a "dedicated" pricing tier | Strategy 2 (separate cluster) |

For launch, neither is needed. The current setup handles many small projects well, and the partition rebalancing procedure covers the first scaling pressure point.

### 2-node quorum limitations

Both ClickHouse Keeper and Redpanda use Raft consensus, which requires an **odd number of nodes** (minimum 3) for fault-tolerant quorum. With only 2 physical nodes, true quorum is not possible. Current trade-offs:

**ClickHouse Keeper** — kept at 1 replica. With 2 Keeper instances, quorum = 2 (both must be up), which is strictly worse than 1 — any Keeper failure blocks all `ReplicatedMergeTree` writes cluster-wide. A single Keeper is a SPOF but works reliably as long as its node is healthy.

**Redpanda** — runs 2 brokers but `replication.factor` stays at 1 on all topics. With 2 brokers, the Raft controller quorum requires both brokers up. Topic data is not replicated — losing a broker loses the partitions it owns.

**Data loss is acceptable** for the current workloads (OTel traces, metrics, logs). These are observability data that can be re-ingested or are ephemeral.

**When a 3rd node is added:**

1. Scale ClickHouse Keeper to 3 replicas (`keeper.yaml` → `spec.replicas: 3`) for proper Raft quorum (tolerates 1 failure)
2. Optionally scale Redpanda to 3 brokers and set `replication.factor=2` on topics for data redundancy
3. Enable Postgres synchronous replication if write durability across nodes is needed

### Recommended cluster sizes

| Cluster size | Layout |
|-------------|--------|
| 1 node | Everything on one machine (dev/test) |
| 2 nodes | Primary (control-plane + CI + infra DBs), Worker (app pods) |
| 3 nodes | Primary (control-plane + CI), Worker (app pods), Storage (ClickHouse + Postgres + Redpanda + Redis) |
| 4+ nodes | Separate storage nodes per service; multiple app workers for horizontal scaling |
