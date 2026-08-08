#!/usr/bin/env bash
# Apply all Argo CD Application manifests so they show up in Argo CD UI.
# Run from repo root. Requires: KUBECONFIG set, git remote origin.
set -e
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
REPO_URL_RAW="$(git remote get-url origin 2>/dev/null || true)"
# Argo CD needs HTTPS (no SSH agent in cluster). Convert git@github.com:owner/repo -> https://github.com/owner/repo
if [[ "$REPO_URL_RAW" =~ ^git@github\.com:(.+)$ ]]; then
  REPO_URL="https://github.com/${BASH_REMATCH[1]}"
elif [[ -z "$REPO_URL_RAW" ]]; then
  REPO_URL=""
else
  REPO_URL="$REPO_URL_RAW"
fi
SERVER_IP="${SERVER_IP:-YOUR_SERVER_IP}"
if [[ -z "$REPO_URL" ]]; then
  echo "Set REPO_URL (e.g. export REPO_URL=https://github.com/your-org/reiver.git)"
  exit 1
fi
echo "Applying Argo CD Applications (REPO_URL=$REPO_URL, SERVER_IP=$SERVER_IP)..."
kubectl apply -f deploy/gitops/argocd/project-reiver.yaml
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-argocd-bootstrap.yaml | kubectl apply -f -
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-infra.yaml | kubectl apply -f -
sed "s|REPLACE_REPO_URL|$REPO_URL|g" deploy/gitops/argocd/application-app-production.yaml | kubectl apply -f -
sed -e "s|REPLACE_REPO_URL|$REPO_URL|g" -e "s|REPLACE_DRONE_HOST|$SERVER_IP|g" deploy/gitops/argocd/application-drone.yaml | kubectl apply -f -
echo "Done. Refresh Argo CD UI to see all apps (argocd-bootstrap, reiver-infra, app-production, drone). The Drone chart deploys the Kubernetes secrets extension when secretsExtension.enabled is true."
