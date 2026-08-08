# Backup and restore (infra)

When running Postgres, ClickHouse, Redis, and Redpanda in-cluster via [deploy/gitops/infra](.), define backups and test restores. Below is a per-component strategy.

## PostgreSQL (CloudNativePG)

- **Backup**: Enable `spec.backup.barmanObjectStore` on the Cluster in [postgres/cluster.yaml](postgres/cluster.yaml). Set `destinationPath` to an S3/R2 bucket path (e.g. `s3://your-bucket/reiver/postgres`). Create a Secret `postgres-backup-creds` in `reiver-infra` with `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` for the object store. Use `retentionPolicy` (e.g. `30d`) for automatic pruning. See [CloudNativePG backup docs](https://cloudnative-pg.io/documentation/current/backup_barmanobjectstore/).
- **Restore**: Use `cnpg restore` or create a new Cluster with `spec.bootstrap.recovery` pointing at the backup destination. Document the exact restore steps for your bucket and cluster name.

## ClickHouse

- **Backup**: Use [clickhouse-backup](https://github.com/Altinity/clickhouse-backup) or ClickHouse native backup to object storage. Run as a CronJob in `reiver-infra` that executes backup and uploads to S3/R2 (e.g. daily). Configure the tool with the same bucket/credentials pattern as Postgres.
- **Restore**: Restore from a backup using clickhouse-backup or native restore; document the procedure and test it once.

## Redis

- **Backup**: Enable RDB persistence (Bitnami Redis chart already uses persistence). Add a CronJob that runs `redis-cli BGSAVE`, then copies the RDB snapshot from the Redis PVC (or from a sidecar) to object storage (S3/R2). Alternatively use a Redis backup sidecar image if available.
- **Restore**: Stop Redis, replace the RDB file on the volume with the restored snapshot, restart. Document and test.

## Redpanda (Kafka)

- **Backup**: Kafka/Redpanda is typically backed by retention and replication rather than full snapshots. Rely on `retention.ms` / `retention.bytes` and replication factor. For disaster recovery, consider exporting critical topics to object storage (e.g. with Kafka Connect or a custom job) and document how to re-create topics and replay.
- **Restore**: Restore from exported data or from a replicated cluster; document the chosen approach.

## Checklist

- [ ] Postgres: barmanObjectStore (or equivalent) enabled and tested; restore tested once.
- [ ] ClickHouse: Backup CronJob or tool configured; restore tested once.
- [ ] Redis: RDB snapshot backup to object storage; restore tested once.
- [ ] Redpanda: Retention and (optional) export strategy documented.

Store backup credentials (e.g. S3/R2 keys) in Kubernetes Secrets or a secret manager; do not commit them to Git.
