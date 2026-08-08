# Continuous Deployment (GitOps)

Reiver uses **GitOps**: the cluster syncs from this Git repo via **Argo CD**. GitHub Actions only builds/pushes images and updates image tags in Git; it does **not** run `kubectl` or hold cluster credentials.

## How It Works

1. You publish a **GitHub Release** (tag + optional pre-release).
2. **Build and push**: GitHub Actions builds the five Docker images (watch, flow, pond, website, mcp) and pushes to ghcr.io with the release tag (and `latest` for production, `dev-latest` for pre-release).
3. **Update Git**: A second job updates the Kustomize overlay in Git (`deploy/k8s/overlays/production` or `dev`) with the image registry prefix and tag, then commits and pushes to the default branch.
4. **Argo CD syncs**: Argo CD (running in the cluster) watches the repo and applies the updated manifests. No `kubectl` from CI; no `KUBECONFIG_*` secrets needed.

- **Pre-release** → dev overlay updated with `dev-latest`; Argo CD app `reiver-app-dev` syncs (if configured).
- **Full release** → production overlay updated with the release version tag; Argo CD app `reiver-app-production` syncs.

## Setup

### 1. Argo CD and Applications

Install Argo CD in the cluster and apply the Application manifests so Argo CD watches this repo. See [deploy/gitops/README.md](gitops/README.md). No GitHub Actions secrets are required for deployment; `GITHUB_TOKEN` is used only for pushing image tags to Git and for ghcr.io push.

### 2. Configure GitHub Environments (optional)

If you use Environments for approval gates, create `dev` and `production` under Settings → Environments. The workflow does not use cluster secrets; environments are optional for branch protection or manual approval.

### 3. Create Kubernetes Secrets (per cluster)

Create the application Secrets in the `reiver` namespace **before** the app is synced. See [deploy/k8s/SECRETS.md](k8s/SECRETS.md). When using in-cluster DBs from [deploy/gitops/infra](gitops/infra), use the in-cluster hostnames listed there.

### 4. Cluster can pull from ghcr.io

Ensure the cluster can pull images (public or via `imagePullSecrets` for private).

## Creating a Release

### Deploy to Dev (Pre-release)

1. GitHub → Releases → Create new release
2. Tag: e.g. `v1.2.3-rc.1`
3. Check **This is a pre-release**
4. Publish → workflow builds/pushes, then updates `deploy/k8s/overlays/dev` in Git; Argo CD syncs.

### Deploy to Production

1. GitHub → Releases → Create new release
2. Tag: e.g. `v1.2.3`
3. Leave **This is a pre-release** unchecked
4. Publish → workflow builds/pushes, then updates `deploy/k8s/overlays/production` with the new image tag and pushes; Argo CD syncs.

## Rollback

- **Revert in Git**: Revert the commit that updated the overlay (e.g. restore previous `newTag` values), push, and let Argo CD sync. Or in Argo CD UI, sync to an older commit.
- **Redeploy**: Create a new release with a known-good tag so the overlay is updated again.

## Deployment Model

Reiver runs as **8 Kubernetes Deployments** from **5 Docker images**. Manifests live in [deploy/k8s/](k8s/) (base + overlays); Argo CD syncs from [deploy/gitops/app/overlays/](gitops/app/overlays/) which reference those overlays. Infra (Postgres, ClickHouse, Redis, Redpanda) is in [deploy/gitops/infra/](gitops/infra/).

## Troubleshooting

### Image tag not updating in cluster

- Confirm the "Update Git with image tags" job ran and pushed a commit (check Actions logs).
- In Argo CD, check that the Application is synced and shows the latest commit; trigger a refresh/sync if needed.

### Image pull fails (ImagePullBackOff)

- Verify ghcr.io images were pushed with the expected tag.
- For private images, add `imagePullSecrets` to the Deployments (e.g. via Kustomize patch).

### Pods stuck or CrashLoopBackOff

- Check `kubectl -n reiver describe pod <pod>` and logs.
- Verify application Secrets exist and match [deploy/k8s/SECRETS.md](k8s/SECRETS.md); when using GitOps infra, use in-cluster DB hostnames.
