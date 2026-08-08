#!/usr/bin/env bash
# Add this repo as a private Git repository in Argo CD so it can clone it.
# Run once when you see "authentication required: Repository not found" for any app.
# Requires: KUBECONFIG, and GITHUB_TOKEN (or ARGOCD_REPO_PASSWORD) with repo read access.
set -e
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
REPO_URL_RAW="$(git remote get-url origin 2>/dev/null || true)"
if [[ "$REPO_URL_RAW" =~ ^git@github\.com:(.+)$ ]]; then
  REPO_URL="https://github.com/${BASH_REMATCH[1]}"
elif [[ -z "$REPO_URL_RAW" ]]; then
  REPO_URL="${REPO_URL:-}"
else
  REPO_URL="${REPO_URL:-$REPO_URL_RAW}"
fi
if [[ -z "$REPO_URL" ]]; then
  echo "Set REPO_URL (e.g. export REPO_URL=https://github.com/your-org/reiver.git)"
  exit 1
fi
PASSWORD="${GITHUB_TOKEN:-${ARGOCD_REPO_PASSWORD:-}}"
if [[ -z "$PASSWORD" ]]; then
  echo "Set GITHUB_TOKEN or ARGOCD_REPO_PASSWORD (GitHub PAT with repo read)."
  exit 1
fi
SECRET_NAME="repo-reiver"
echo "Adding repository to Argo CD: $REPO_URL"
kubectl create secret generic "$SECRET_NAME" -n argocd \
  --from-literal=type=git \
  --from-literal=url="$REPO_URL" \
  --from-literal=username=git \
  --from-literal=password="$PASSWORD" \
  --dry-run=client -o yaml | \
kubectl label -f - argocd.argoproj.io/secret-type=repository --local -o yaml | \
kubectl apply -f -
echo "Done. Argo CD will use this credential to clone the repo; sync should succeed now."