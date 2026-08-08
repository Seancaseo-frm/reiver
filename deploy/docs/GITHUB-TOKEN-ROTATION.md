# Rotating GitHub Personal Access Tokens

A single GitHub PAT (with `repo` + `write:packages` scopes) is used across four Kubernetes secrets in three namespaces. When you rotate (revoke + recreate) the PAT, **all four** must be updated or the pipeline and deployments will break in different ways.

## Token scopes required

| Scope | Why |
|-------|-----|
| `repo` | Clone the private repository (Drone clone step, Argo CD sync) |
| `write:packages` | Push container images to `ghcr.io` (Kaniko) and pull them (kubelet) |

A single PAT with both scopes covers every use-case.

## Secrets to update

| # | Secret name | Namespace | Purpose | Failure symptom if stale |
|---|-------------|-----------|---------|--------------------------|
| 1 | `git-push-token` | `drone` | Drone clone step (`git clone`) | Clone silently fails → subsequent steps error with "can't cd to …" or "No such file or directory" |
| 2 | `registry-password` | `drone` | Kaniko pushes images to `ghcr.io` | `error checking push permissions … DENIED` |
| 3 | `repo-reiver` | `argocd` | Argo CD clones the repo for GitOps sync | "authentication required: Repository not found" in Argo CD |
| 4 | `ghcr.io-pull-secret` | `reiver` | Kubelet pulls private images from `ghcr.io` | `ImagePullBackOff` / "failed to authorize … 403 Forbidden" |

## Rotation commands

Run these after creating a new GitHub PAT. Replace `<NEW_PAT>` with the token value.

### 1. Drone — git clone token

```bash
kubectl delete secret git-push-token -n drone
kubectl create secret generic git-push-token -n drone \
  --from-literal=git-push-token="<NEW_PAT>"
```

### 2. Drone — container registry push

```bash
kubectl delete secret registry-password -n drone
kubectl create secret generic registry-password -n drone \
  --from-literal=registry-password="<NEW_PAT>"
```

`registry-username` rarely changes, but verify it matches your GitHub username:

```bash
kubectl delete secret registry-username -n drone
kubectl create secret generic registry-username -n drone \
  --from-literal=registry-username="your-org"
```

### 3. Argo CD — repo clone

```bash
GITHUB_TOKEN="<NEW_PAT>" bash deploy/scripts/argocd-add-repo-secret.sh
```

Or copy from the Drone secret you just set (avoids pasting the token again):

```bash
GITHUB_TOKEN="$(kubectl get secret git-push-token -n drone \
  -o jsonpath='{.data.git-push-token}' | base64 -d)" \
  bash deploy/scripts/argocd-add-repo-secret.sh
```

### 4. Kubelet — image pull from ghcr.io

```bash
TOKEN="$(kubectl get secret git-push-token -n drone \
  -o jsonpath='{.data.git-push-token}' | base64 -d)"

kubectl create secret docker-registry ghcr.io-pull-secret -n reiver \
  --docker-server=ghcr.io \
  --docker-username=your-org \
  --docker-password="$TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
```

After updating the pull secret, **delete all pods that are stuck** so the deployment controller recreates them with the new credentials. Existing pods cache the old `imagePullSecrets` from when they were created — updating the Secret alone is not enough; the pods must be recreated.

```bash
# Find and delete all pods stuck on image pull
kubectl get pods -n reiver --no-headers | grep -v -E "Running|Completed|Terminating" | awk '{print $1}' | xargs -r kubectl delete pod -n reiver

# Or restart all deployments at once (rolling restart, no downtime)
kubectl rollout restart deployment -n reiver
```

The rolling restart is the safest option — it recreates every pod one-by-one, and each new pod picks up the updated `ghcr.io-pull-secret` automatically.

## All-in-one script

Paste the new PAT once and update everything:

```bash
#!/usr/bin/env bash
set -euo pipefail

read -rsp "New GitHub PAT: " NEW_PAT; echo
GITHUB_USER="your-org"

echo "1/4  Updating git-push-token (drone)…"
kubectl delete secret git-push-token -n drone --ignore-not-found
kubectl create secret generic git-push-token -n drone \
  --from-literal=git-push-token="$NEW_PAT"

echo "2/4  Updating registry-password (drone)…"
kubectl delete secret registry-password -n drone --ignore-not-found
kubectl create secret generic registry-password -n drone \
  --from-literal=registry-password="$NEW_PAT"

echo "3/4  Updating repo-reiver (argocd)…"
GITHUB_TOKEN="$NEW_PAT" bash deploy/scripts/argocd-add-repo-secret.sh

echo "4/4  Updating ghcr.io-pull-secret (reiver)…"
kubectl create secret docker-registry ghcr.io-pull-secret -n reiver \
  --docker-server=ghcr.io \
  --docker-username="$GITHUB_USER" \
  --docker-password="$NEW_PAT" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "5/5  Rolling restart of all reiver deployments…"
kubectl rollout restart deployment -n reiver

echo "Done. All four secrets updated and deployments restarted."
```

## Verification (without exposing the token)

```bash
# Check all secrets exist and have non-empty values
kubectl get secret git-push-token   -n drone     -o jsonpath='{.data.git-push-token}'   | base64 -d | wc -c
kubectl get secret registry-password -n drone     -o jsonpath='{.data.registry-password}' | base64 -d | wc -c
kubectl get secret repo-reiver       -n argocd    -o jsonpath='{.data.password}'          | base64 -d | wc -c
kubectl get secret ghcr.io-pull-secret -n reiver -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d | jq '.auths["ghcr.io"].password' | wc -c
```

All should print a character count > 0 (classic PATs are 40 characters; fine-grained tokens are longer).

## Related docs

- [deploy/docs/DRONE.md](DRONE.md) — full Drone CI setup including secret creation
- [deploy/k8s/SECRETS.md](../k8s/SECRETS.md) — all application Kubernetes secrets
- [deploy/scripts/argocd-add-repo-secret.sh](../scripts/argocd-add-repo-secret.sh) — Argo CD repo credential helper
