# Step-by-step setup (GitOps + in-cluster DBs)

This guide assumes you want **one Kubernetes cluster** with **databases and app in-cluster** (no separate DB VM). Follow in order.

---

## Step 1: Create a Kubernetes cluster

- On **Hetzner** (or elsewhere): create one or more VMs, install Kubernetes (e.g. k3s: `curl -sfL https://get.k3s.io | sh`).
- From your laptop: copy kubeconfig and confirm `kubectl get nodes` works.
- Ensure the cluster can pull images from **ghcr.io** (for Reiver images and Helm charts).

**Optional (Hetzner single-server):** For initial server setup (k3s + kubeconfig + Argo CD + Applications in one go), run from the repo root: `./deploy/scripts/setup-server.sh`. The script tries SSH key auth first and falls back to password. You can override the server with `SERVER_IP=...` or use [deploy/scripts/servers.conf](scripts/servers.conf) (first non-comment line = primary server). Then continue from Step 4 (wait for infra) and Step 6 (create Secrets). See [deploy/scripts/setup-server.sh](scripts/setup-server.sh).

**Adding worker nodes:** To add a new server to an existing cluster: `./deploy/scripts/setup-server.sh add-node <IP> [user]`. This installs k3s in agent mode and joins the node automatically.

**Ref**: [deploy/hetzner/README.md](hetzner/README.md) § 1.1, 1.3.

---

## Step 2: Install Argo CD

From a machine with `kubectl` and cluster access:

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

Wait until pods are ready:

```bash
kubectl -n argocd get pods
```

(Optional) Get the initial admin password to log in to the UI:

```bash
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d && echo
```

**Ref**: [deploy/gitops/README.md](gitops/README.md) § Bootstrap.

---

## Step 3: Point Argo CD at this repo

Set your Git repo URL and apply the Application manifests (from the **repo root**):

```bash
export REPO_URL="https://github.com/YOUR_ORG/YOUR_REPO.git"

sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-infra.yaml | kubectl apply -f -
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-app-production.yaml | kubectl apply -f -
```

For a **private** repo: in Argo CD UI go to **Settings → Repositories** and add the repo (HTTPS + token or SSH key).

Argo CD will start syncing. Infra (Postgres, ClickHouse, Redis, Redpanda) will deploy first; the app will deploy once infra is present.

**Ref**: [deploy/gitops/README.md](gitops/README.md) § 2–3.

---

## Step 4: Wait for infra to be healthy

In Argo CD UI (or CLI), open the **reiver-infra** Application. Wait until it is **Synced** and **Healthy**. This installs:

- CloudNativePG operator + Postgres cluster (database `reiver`)
- ClickHouse, Redis, Redpanda (Helm charts)

Check pods:

```bash
kubectl -n reiver-infra get pods
```

**Ref**: [deploy/gitops/infra/README.md](gitops/infra/README.md).

---

## Step 5: Get Postgres password for app Secrets

The Postgres superuser password is in a secret created by CloudNativePG:

```bash
kubectl -n reiver-infra get secret postgres-cluster-superuser -o jsonpath='{.data.password}' | base64 -d && echo
```

Use this password (and username from the same secret, usually `postgres`) in `DATABASE_URL` in the next step. Alternatively create a dedicated app user and use that in `DATABASE_URL` (see [deploy/gitops/infra/README.md](gitops/infra/README.md)).

---

## Step 6: Create the four app Secrets

Create these Secrets in the **reiver** namespace **before** (or right after) the app syncs. Use the **in-cluster** hostnames and the Postgres password from Step 5.

> **Note:** The MCP server (`reiver-mcp`) does not need a Secret. Its service URLs are set directly in the Deployment manifest.

Replace placeholders: `YOUR_POSTGRES_PASSWORD`, `YOUR_JWT_SECRET`, `YOUR_ENCRYPTION_KEY`, and for Pond `YOUR_R2_*` if you use R2.

**Test deployment (no public signup):** Registration is disabled in code. The migration `011_seed_test_users.sql` pre-seeds a dev user (`dev@example.com`). Change the password after first login.

```bash
# Watch (and watch-workers)
kubectl create secret generic reiver-watch -n reiver \
  --from-literal=DATABASE_URL='postgresql://postgres:YOUR_POSTGRES_PASSWORD@postgres-cluster-rw.reiver-infra.svc:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@clickhouse.reiver-infra.svc:8123' \
  --from-literal=REDIS_URL='redis://redis-master.reiver-infra.svc:6379' \
  --from-literal=KAFKA_HOSTS='redpanda.reiver-infra.svc:9092' \
  --from-literal=JWT_SECRET='YOUR_JWT_SECRET' \
  --from-literal=ENCRYPTION_KEY='YOUR_ENCRYPTION_KEY'

# Flow (same as watch, no R2)
kubectl create secret generic reiver-flow -n reiver \
  --from-literal=DATABASE_URL='postgresql://postgres:YOUR_POSTGRES_PASSWORD@postgres-cluster-rw.reiver-infra.svc:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@clickhouse.reiver-infra.svc:8123' \
  --from-literal=REDIS_URL='redis://redis-master.reiver-infra.svc:6379' \
  --from-literal=KAFKA_HOSTS='redpanda.reiver-infra.svc:9092' \
  --from-literal=JWT_SECRET='YOUR_JWT_SECRET' \
  --from-literal=ENCRYPTION_KEY='YOUR_ENCRYPTION_KEY' \
  --from-literal=ENVIRONMENT='production' \
  --from-literal=TRUSTED_PROXY_CIDRS='10.42.0.0/16,10.43.0.0/16'

# Pond (and pond-workers) — add your R2 credentials
kubectl create secret generic reiver-pond -n reiver \
  --from-literal=DATABASE_URL='postgresql://postgres:YOUR_POSTGRES_PASSWORD@postgres-cluster-rw.reiver-infra.svc:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@clickhouse.reiver-infra.svc:8123' \
  --from-literal=REDIS_URL='redis://redis-master.reiver-infra.svc:6379' \
  --from-literal=KAFKA_HOSTS='redpanda.reiver-infra.svc:9092' \
  --from-literal=JWT_SECRET='YOUR_JWT_SECRET' \
  --from-literal=ENCRYPTION_KEY='YOUR_ENCRYPTION_KEY' \
  --from-literal=R2_BUCKET='YOUR_R2_BUCKET' \
  --from-literal=R2_ENDPOINT='YOUR_R2_ENDPOINT' \
  --from-literal=R2_ACCESS_KEY_ID='YOUR_R2_ACCESS_KEY_ID' \
  --from-literal=R2_SECRET_ACCESS_KEY='YOUR_R2_SECRET_ACCESS_KEY'

# ClickHouse R2 credentials (tiered storage cold tier) — reuses Pond's R2 credentials
# CH_R2_ENDPOINT is the full S3 path: https://{account_id}.r2.cloudflarestorage.com/{bucket}/clickhouse/
kubectl create secret generic clickhouse-r2-credentials -n reiver-infra \
  --from-literal=R2_ACCESS_KEY_ID="$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_ACCESS_KEY_ID}' | base64 -d)" \
  --from-literal=R2_SECRET_ACCESS_KEY="$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_SECRET_ACCESS_KEY}' | base64 -d)" \
  --from-literal=CH_R2_ENDPOINT="https://$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_ACCOUNT_ID}' | base64 -d).r2.cloudflarestorage.com/$(kubectl get secret reiver-pond -n reiver -o jsonpath='{.data.R2_BUCKET}' | base64 -d)/clickhouse/"

# Website (and website-workers) — in-cluster URLs for Watch, Flow, Pond
kubectl create secret generic reiver-website -n reiver \
  --from-literal=DATABASE_URL='postgresql://postgres:YOUR_POSTGRES_PASSWORD@postgres-cluster-rw.reiver-infra.svc:5432/reiver' \
  --from-literal=CLICKHOUSE_URL='http://default:@clickhouse.reiver-infra.svc:8123' \
  --from-literal=REDIS_URL='redis://redis-master.reiver-infra.svc:6379' \
  --from-literal=JWT_SECRET='YOUR_JWT_SECRET' \
  --from-literal=ENCRYPTION_KEY='YOUR_ENCRYPTION_KEY' \
  --from-literal=WATCH_URL='http://reiver-watch:3000' \
  --from-literal=FLOW_URL='http://reiver-flow:3001' \
  --from-literal=POND_URL='http://reiver-pond:3002'
```

**Ref**: [deploy/k8s/SECRETS.md](k8s/SECRETS.md).

---

## Step 7: Set image tags in Git (first deploy)

The app overlay in Git uses placeholders. Either:

- **Option A**: Publish a **GitHub Release** (e.g. tag `v0.1.0`). The deploy workflow will build images, push them, and **update the overlay in Git** with your registry and tag; Argo CD will then sync.  
  **Or**
- **Option B**: Manually edit `deploy/k8s/overlays/production/kustomization.yaml`: replace `REPLACE_REGISTRY_PREFIX` with your image prefix (e.g. `ghcr.io/your-org/reiver`) and set `newTag` to an existing image tag (e.g. `latest`). Commit and push so Argo CD syncs.

After sync, the **reiver-app-production** Application should be Healthy and the seven Reiver workloads should be running.

**Ref**: [deploy/CD.md](CD.md), [deploy/k8s/README.md](k8s/README.md).

---

## Step 8: Verify

```bash
kubectl -n reiver get pods,svc
```

All deployments (watch, watch-workers, flow, pond, pond-workers, website, website-workers, mcp) should be Running. Optionally expose the website (Ingress or LoadBalancer on port 3003); see [deploy/k8s/README.md](k8s/README.md) “Exposing the API (optional)”.

---

## Step 9 (optional): Backups

If using in-cluster DBs, configure backups for Postgres, ClickHouse, Redis, and Redpanda. See [deploy/gitops/infra/BACKUPS.md](gitops/infra/BACKUPS.md).

---

## Summary

| Step | What |
|------|------|
| 1 | Create Kubernetes cluster |
| 2 | Install Argo CD |
| 3 | Apply Application manifests (repo URL set) |
| 4 | Wait for infra (reiver-infra) to be Healthy |
| 5 | Get Postgres password from `postgres-cluster-superuser` |
| 6 | Create four app Secrets in `reiver` (MCP needs no secret) |
| 7 | Set image tags (release or manual edit) so app can sync |
| 8 | Verify all 8 deployments are running, optional Ingress |
| 9 | (Optional) Configure backups |

For cloud-specific details (e.g. Hetzner) and other options (separate DB VM, dev overlay), see [deploy/hetzner/README.md](hetzner/README.md) and [deploy/gitops/README.md](gitops/README.md).
