# Hetzner Cloud Setup and Deployments

This runbook describes how to set up Reiver on Hetzner Cloud with **GitOps** (Argo CD). The cluster syncs from this repo; databases and the app can run **in-cluster** via [deploy/gitops/infra](../gitops/infra/) and [deploy/gitops/app](../gitops/app/). Object storage in production is **R2** (no MinIO in-cluster for prod).

**For a single, ordered checklist**, use [deploy/SETUP.md](../SETUP.md) (steps 1–9).

---


## Part 1: Initial setup on Hetzner Cloud

Target topology: **Kubernetes cluster** on one or more VMs. Data services (Postgres, ClickHouse, Redis, Redpanda) can run **in-cluster** (recommended) via GitOps infra.

### 1.1 Create VMs and network

1. In **Hetzner Cloud Console** (or API): Create a project and enable a **Private Network** (e.g. `10.0.0.0/16`) if you have multiple VMs.
2. **K8s VM(s)**: Create one or more Cloud servers for Kubernetes (e.g. CPX31 or larger). Attach the private network if used. This cluster will run Reiver app and, if using GitOps infra, the databases as well. Open ports for the Kubernetes API (see 1.5) and for user-facing traffic (e.g. 80/443).

**Record**: VM sizes, K8s API endpoint.

### 1.2 Database tier 

– In-cluster (GitOps infra, recommended)**

- No separate DB VM. Deploy [deploy/gitops/infra](../gitops/infra/) via Argo CD; Postgres, ClickHouse, Redis, and Redpanda run in namespace `reiver-infra`. See [deploy/gitops/README.md](../gitops/README.md) and [deploy/gitops/infra/README.md](../gitops/infra/README.md). App Secrets use in-cluster hostnames from [deploy/k8s/SECRETS.md](../k8s/SECRETS.md).

### 1.3 Kubernetes cluster and GitOps

1. **Install Kubernetes** on the K8s VM(s), e.g. k3s (`curl -sfL https://get.k3s.io | sh`). Ensure `kubectl` works and the cluster can pull images from ghcr.io.
2. **Bootstrap Argo CD**: Install Argo CD and apply the Application manifests so it watches this repo. See [deploy/gitops/README.md](../gitops/README.md). Replace `REPLACE_REPO_URL` in the Application manifests with your repo URL.

   **One-shot script (single or multi-server):** From the repo root, run `./deploy/scripts/setup-server.sh` and enter the server password when prompted. This installs k3s on the primary server, fetches kubeconfig, installs Argo CD, applies the Application manifests (including Woodpecker CI), and prints join commands if you have multiple servers in `deploy/scripts/servers.conf` (first line = primary; add one line per extra node). Then create app Secrets (Step 6 in [deploy/SETUP.md](../SETUP.md)) and, for Woodpecker, follow [deploy/docs/WOODPECKER.md](../docs/WOODPECKER.md).

3. **Sync**: Argo CD will sync `deploy/gitops/infra` (if using in-cluster DBs) and `deploy/gitops/app/overlays/production` (or dev). Ensure app Secrets exist in `reiver` before or right after the app syncs (see [deploy/k8s/SECRETS.md](../k8s/SECRETS.md)).

### 1.4 Kubernetes Secrets (app and optional DB credentials)

Create the four application Secrets in the `reiver` namespace **before** the app is fully used. The MCP server (`reiver-mcp`) does not need a Secret — its service URLs are set in the Deployment manifest. When using in-cluster DBs, use the hostnames from the “In-cluster databases” section of [deploy/k8s/SECRETS.md](../k8s/SECRETS.md). For Postgres, use the `postgres-cluster-superuser` secret in `reiver-infra` or create an app user. Production uses **R2** for Pond (`R2_*` in `reiver-pond` and `reiver-website` if needed).

### 1.5 Expose Kubernetes API (optional, for manual access)

If you need to run `kubectl` or access Argo CD UI from outside the cluster: expose the Kubernetes API (and optionally Argo CD) with HTTPS. No GitHub Actions secrets are required for deployment; CI only pushes image tag updates to Git, and Argo CD syncs from the repo.

### 1.6 (Optional) Public access to the app

Add an Ingress or LoadBalancer for the `reiver-website` Service (port 3003). See [deploy/k8s/README.md](../k8s/README.md) “Exposing the API (optional)”.

### 1.7 Backups

See [deploy/gitops/infra/BACKUPS.md](../gitops/infra/BACKUPS.md) for backup strategy and restore steps for Postgres, ClickHouse, Redis, and Redpanda when using in-cluster infra.

---

## Part 2: Regular deployments (GitOps)

1. **Trigger**: Publish a **GitHub Release** (tag + optional pre-release).
2. **Workflow** ([.github/workflows/deploy.yml](../../.github/workflows/deploy.yml)): Builds and pushes Docker images to ghcr.io, then **updates the Kustomize overlay in Git** (image registry prefix and tag) and pushes to the default branch. **Argo CD** syncs the repo and applies changes; no `kubectl` or KUBECONFIG in CI.
3. **Rollback**: Revert the overlay commit in Git and sync, or create a new release with a known-good tag. See [deploy/CD.md](../CD.md).

---

## First-time setup checklist

- [ ] Hetzner project (and optional private network) created
- [ ] K8s VM(s) created; Kubernetes installed; `kubectl` works
- [ ] Argo CD installed; Application manifests applied with repo URL set (see [deploy/gitops/README.md](../gitops/README.md))
- [ ] Infra synced (if using in-cluster DBs): `reiver-infra` namespace and Postgres, ClickHouse, Redis, Redpanda running
- [ ] App Secrets created in `reiver` (see [deploy/k8s/SECRETS.md](../k8s/SECRETS.md)); website uses WATCH_URL, FLOW_URL, POND_URL
- [ ] App synced: `reiver-app-production` (and optionally `reiver-app-dev`) Healthy in Argo CD (includes MCP deployment)
- [ ] Backups configured (see [deploy/gitops/infra/BACKUPS.md](../gitops/infra/BACKUPS.md)) if using in-cluster DBs
