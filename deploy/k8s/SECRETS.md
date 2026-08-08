# Kubernetes Secrets for Reiver

Secrets are **not** stored in Git. Create them in the `reiver` namespace before deploying. Use `kubectl create secret generic` or your preferred secret manager (e.g. Sealed Secrets, External Secrets).

All values below are placeholders; replace with real connection strings and keys.

## In-cluster databases (GitOps infra)

When using [deploy/gitops/infra](../gitops/infra/) (Postgres, ClickHouse, Redis, Redpanda in namespace `reiver-infra`), use these hostnames in your connection strings:

| Service | Host:port | Use in |
|---------|-----------|--------|
| Postgres (read-write) | `postgres-cluster-rw.reiver-infra.svc:5432` | `DATABASE_URL` |
| ClickHouse HTTP | `clickhouse.reiver-infra.svc:8123` | `CLICKHOUSE_URL` → `http://default:@clickhouse.reiver-infra.svc:8123` |
| Redis | `redis-master.reiver-infra.svc:6379` | `REDIS_URL` → `redis://redis-master.reiver-infra.svc:6379` |
| Redpanda (Kafka) | `redpanda.reiver-infra.svc:9092` (or the port the chart exposes) | `KAFKA_HOSTS` |

Postgres credentials: use the `postgres-cluster-superuser` secret in `reiver-infra`, or create an app user and store its password in your app Secret. See [deploy/gitops/infra/README.md](../gitops/infra/README.md).

---

## Secret: `reiver-watch`

Used by: `reiver-watch` (API) and `reiver-watch-workers`.

| Key | Description |
|-----|-------------|
| `DATABASE_URL` | PostgreSQL connection string, e.g. `postgresql://user:pass@host:5432/reiver` |
| `CLICKHOUSE_URL` | ClickHouse HTTP URL, e.g. `http://default:@host:8123` |
| `REDIS_URL` | Redis URL, e.g. `redis://host:6379` |
| `KAFKA_HOSTS` | Kafka brokers, e.g. `host:9092` |
| `JWT_SECRET` | Secret for JWT signing |
| `ENCRYPTION_KEY` | Key for encrypting sensitive data |

**Example:**

```bash
kubectl create secret generic reiver-watch -n reiver \
  --from-literal=DATABASE_URL='postgresql://user:pass@db:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@ch:8123' \
  --from-literal=REDIS_URL='redis://redis:6379' \
  --from-literal=KAFKA_HOSTS='kafka:9092' \
  --from-literal=JWT_SECRET='your-jwt-secret' \
  --from-literal=ENCRYPTION_KEY='your-encryption-key'
```

---

## Secret: `reiver-flow`

Used by: `reiver-flow`.

Same keys as `reiver-watch`: `DATABASE_URL`, `CLICKHOUSE_URL`, `REDIS_URL`, `KAFKA_HOSTS`, `JWT_SECRET`, `ENCRYPTION_KEY`.

Additional required keys:

| Key | Description |
|-----|-------------|
| `ENVIRONMENT` | Set to `production` in production deployments |
| `TRUSTED_PROXY_CIDRS` | Comma-separated CIDR blocks of trusted proxies (website pods). Required when `ENVIRONMENT=production`. Use your cluster's pod/service CIDR, e.g. `10.42.0.0/16,10.43.0.0/16` |

---

## Secret: `reiver-pond`

Used by: `reiver-pond` (API) and `reiver-pond-workers`.

| Key | Description |
|-----|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `CLICKHOUSE_URL` | ClickHouse HTTP URL |
| `REDIS_URL` | Redis URL |
| `KAFKA_HOSTS` | Kafka brokers |
| `JWT_SECRET` | JWT signing secret |
| `ENCRYPTION_KEY` | Encryption key |
| `R2_BUCKET` | R2 (or S3-compatible) bucket name, e.g. `warehouse` |
| `R2_ENDPOINT` | R2/S3 endpoint URL, e.g. `https://...r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY_ID` | R2 access key |
| `R2_SECRET_ACCESS_KEY` | R2 secret key |

`PGWIRE_LISTEN_ADDR` is set in the Deployment manifest to `0.0.0.0:5433`; no need to put it in the Secret.

Optional: for PgWire TLS, add `PGWIRE_TLS_CERT` and `PGWIRE_TLS_KEY` (or mount a TLS secret and reference via env).

---

## Secret: `reiver-website`

Used by: `reiver-website` (API) and `reiver-website-workers`.

| Key | Description |
|-----|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `CLICKHOUSE_URL` | ClickHouse HTTP URL |
| `REDIS_URL` | Redis URL |
| `JWT_SECRET` | JWT signing secret |
| `ENCRYPTION_KEY` | Encryption key |
| `WATCH_URL` | In-cluster URL to Watch API: `http://reiver-watch:3000` |
| `FLOW_URL` | In-cluster URL to Flow API: `http://reiver-flow:3001` |
| `POND_URL` | In-cluster URL to Pond API: `http://reiver-pond:3002` |
| `LOOPS_API_KEY` | *(optional)* Loops.so API key — email sending disabled when unset |
| `LOOPS_INVITE_TEMPLATE_ID` | *(optional)* Loops transactional ID for org invite emails |
| `LOOPS_ALERT_TEMPLATE_ID` | *(optional)* Loops transactional ID for alert notification emails |
| `LOOPS_WELCOME_TEMPLATE_ID` | *(optional)* Loops transactional ID for welcome emails on signup |
| `APP_URL` | *(optional)* Public base URL for invite links, e.g. `https://app.reiver.ai` |

**Example:**

```bash
kubectl create secret generic reiver-website -n reiver \
  --from-literal=DATABASE_URL='postgresql://user:pass@db:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@ch:8123' \
  --from-literal=REDIS_URL='redis://redis:6379' \
  --from-literal=JWT_SECRET='your-jwt-secret' \
  --from-literal=ENCRYPTION_KEY='your-encryption-key' \
  --from-literal=WATCH_URL='http://reiver-watch:3000' \
  --from-literal=FLOW_URL='http://reiver-flow:3001' \
  --from-literal=POND_URL='http://reiver-pond:3002' \
  --from-literal=LOOPS_API_KEY='your-loops-api-key' \
  --from-literal=LOOPS_INVITE_TEMPLATE_ID='clxxxxxxxx' \
  --from-literal=LOOPS_ALERT_TEMPLATE_ID='clxxxxxxxx' \
  --from-literal=LOOPS_WELCOME_TEMPLATE_ID='clxxxxxxxx' \
  --from-literal=APP_URL='https://app.reiver.ai'
```

---

## Secret: `reiver-herd`

Used by: `reiver-herd`.

REST/UI routes are authenticated by the website proxy (JWT → trusted `X-Project-Id` / `X-Organization-Id` headers). The `/a2a` protocol route receives the agent's Bearer token directly and validates it by calling back to the website's `/api/auth/validate-key` endpoint. `WEBSITE_URL` is set directly in the Deployment manifest (not the secret), same as MCP.

| Key | Description |
|-----|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `CLICKHOUSE_URL` | ClickHouse HTTP URL |
| `KAFKA_HOSTS` | Kafka brokers |

**Example:**

```bash
kubectl create secret generic reiver-herd -n reiver \
  --from-literal=DATABASE_URL='postgresql://user:pass@db:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@ch:8123' \
  --from-literal=KAFKA_HOSTS='kafka:9092'
```

---

## Secret: `reiver-mcp` (optional)

Used by: `reiver-mcp`.

The MCP server's service URLs (`WEBSITE_URL`, `FLOW_URL`, `WATCH_URL`) and `OTEL_EXPORTER_OTLP_ENDPOINT` are set directly in the Deployment manifest. Authentication is handled per-request by the website proxy. The optional secret provides `OTEL_PROJECT_ID` to enable OpenTelemetry export to Watch.

| Key | Description |
|-----|-------------|
| `OTEL_PROJECT_ID` | Watch project UUID to store MCP telemetry under |

`OTEL_EXPORTER_OTLP_ENDPOINT` is already set in the Deployment manifest to `http://reiver-watch:3000`. If `OTEL_PROJECT_ID` is not set, MCP falls back to console-only logging.

**Example:**

```bash
kubectl create secret generic reiver-mcp -n reiver \
  --from-literal=OTEL_PROJECT_ID='<watch-project-uuid>'
```

---

## Secret: `otel-collector`

Used by: `otel-collector` DaemonSet (OpenTelemetry Collector for K8s infrastructure metrics and container logs).

| Key | Description |
|-----|-------------|
| `OTEL_PROJECT_ID` | Watch project UUID for Kubernetes infrastructure telemetry |

**Example:**

```bash
kubectl create secret generic otel-collector -n reiver \
  --from-literal=OTEL_PROJECT_ID='<watch-project-uuid>'
```

---

## Secret: `otel-project-id` (Drone namespace)

Used by: Drone CI pipeline steps (namespace `drone`).

The CI pipeline uses `otel-cli` to emit OpenTelemetry traces for build and deployment visibility. It needs the same project UUID used by the OTel collector, but in the `drone` namespace (Drone's K8s secrets extension cannot cross namespaces).

| Key | Description |
|-----|-------------|
| `otel-project-id` | Same Watch project UUID as `otel-collector` secret's `OTEL_PROJECT_ID` |

**Example:**

```bash
# Copy the value from the existing otel-collector secret
OTEL_PID=$(kubectl get secret otel-collector -n reiver -o jsonpath='{.data.OTEL_PROJECT_ID}' | base64 -d)

kubectl create secret generic otel-project-id -n drone \
  --from-literal=otel-project-id="$OTEL_PID"
```

---

## Secret: `clickhouse-r2-credentials` (infra namespace)

Used by: ClickHouse pods (tiered storage — cold tier on Cloudflare R2).

| Key | Description |
|-----|-------------|
| `R2_ACCESS_KEY_ID` | R2 access key (same credentials as Pond) |
| `R2_SECRET_ACCESS_KEY` | R2 secret key (same credentials as Pond) |
| `CH_R2_ENDPOINT` | Full S3 endpoint for ClickHouse disk: `https://{account}.r2.cloudflarestorage.com/{bucket}/clickhouse/` |

**Create by piping values from the Pond secret** (avoids exposing credentials). The `CH_R2_ENDPOINT` is composed from Pond's `R2_ACCOUNT_ID` and `R2_BUCKET` with a `/clickhouse/` prefix to isolate ClickHouse data:

```bash
kubectl create secret generic clickhouse-r2-credentials -n reiver-infra \
  --from-literal=R2_ACCESS_KEY_ID="$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_ACCESS_KEY_ID}' | base64 -d)" \
  --from-literal=R2_SECRET_ACCESS_KEY="$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_SECRET_ACCESS_KEY}' | base64 -d)" \
  --from-literal=CH_R2_ENDPOINT="https://$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_ACCOUNT_ID}' | base64 -d).r2.cloudflarestorage.com/$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_BUCKET}' | base64 -d)/clickhouse/"
```

---

## Summary

| Secret name | Namespace | Workloads |
|-------------|-----------|-----------|
| `reiver-watch` | reiver | reiver-watch, reiver-watch-workers |
| `reiver-flow` | reiver | reiver-flow |
| `reiver-pond` | reiver | reiver-pond, reiver-pond-workers |
| `reiver-website` | reiver | reiver-website, reiver-website-workers |
| `reiver-herd` | reiver | reiver-herd |
| `reiver-mcp` *(optional)* | reiver | reiver-mcp (OTEL config) |
| `otel-collector` | reiver | otel-collector DaemonSet (K8s infra telemetry) |
| `otel-project-id` | drone | Drone CI pipeline (build/deploy tracing) |
| `clickhouse-r2-credentials` | reiver-infra | ClickHouse pods (tiered storage cold tier) |

Ensure the namespace `reiver` exists (it is created by the base Kustomize manifests) before creating secrets. The `drone` namespace is created by the Drone Helm chart.

---

## Rotating the GitHub PAT

The GitHub PAT is shared across four secrets in `drone`, `argocd`, and `reiver` namespaces. When you rotate it, all four must be updated. See [deploy/docs/GITHUB-TOKEN-ROTATION.md](../docs/GITHUB-TOKEN-ROTATION.md) for the full procedure.
