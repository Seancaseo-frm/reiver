# GitOps (Argo CD)

This directory is the source of truth for the Kubernetes cluster. **Argo CD** syncs from this repo; no `kubectl apply` from CI.

**New to this setup?** See the linear guide: [deploy/SETUP.md](../SETUP.md) (steps 1–9).

## Layout

- **`argocd/`** — Argo CD Application manifests (infra, app-production, app-dev). Apply these after Argo CD is installed so it watches the repo.
- **`infra/`** — Operators and data services (PostgreSQL, ClickHouse, Redis, Redpanda, Jaeger) in namespace `reiver-infra`. See [infra/README.md](infra/README.md).
- **`app/overlays/`** — Thin wrappers that point at [../k8s/](../k8s/) overlays; Argo CD syncs production or dev from here.

## Bootstrap (one-time)

### 1. Install Argo CD

From a machine with `kubectl` and cluster access:

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
# Wait for pods: kubectl -n argocd get pods
# Get initial admin password: kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
```

Or install via Helm:

```bash
helm repo add argo https://argoproj.github.io/argo-helm
helm install argocd argo/argo-cd -n argocd --create-namespace
```

### 2. Replace repo URL in Application manifests

Edit the Application manifests in `argocd/` and replace `REPLACE_REPO_URL` with your Git repo URL (e.g. `https://github.com/your-org/reiver.git`). For a **private** repo, run once: `GITHUB_TOKEN=your_pat ./deploy/scripts/argocd-add-repo-secret.sh` (or add the repo in Argo CD UI: Settings → Repositories).

### 3. Apply the Applications

From the repo root:

```bash
export REPO_URL="https://github.com/OWNER/REPO.git"
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-infra.yaml | kubectl apply -f -
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-app-production.yaml | kubectl apply -f -
# Optional, for dev:
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-app-dev.yaml | kubectl apply -f -
```

Argo CD will sync `deploy/gitops/infra` and `deploy/gitops/app/overlays/production` (and dev if applied) from the default branch.

### 4. App Secrets (not in Git)

Application Secrets (e.g. `reiver-watch`, `reiver-website`) are **not** stored in Git. Create them once per cluster in the `reiver` namespace with in-cluster DB hostnames. See [deploy/k8s/SECRETS.md](../k8s/SECRETS.md). Infra DB credentials (Postgres user, Redis password if any) are also created out-of-band or via Sealed Secrets / External Secrets.

## Day-to-day

- **Change infra or app**: Edit manifests under `infra/` or `deploy/k8s/`, commit and push. Argo CD will sync (default ~3 min, or trigger sync in the UI).
- **Rollback**: Revert the commit and push, or in Argo CD UI use "Hard Refresh" and "Sync" to an older commit.
- **Manual sync**: In Argo CD UI, open the Application and click "Sync", or: `argocd app sync reiver-infra` (if using Argo CD CLI).

## Sync policy

The provided Application manifests use **auto-sync** with **prune** and **selfHeal** so the cluster stays aligned with Git. To use manual sync, remove the `syncPolicy.automated` block and sync from the UI or CLI when needed.
