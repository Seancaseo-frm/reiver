#!/usr/bin/env bash
# One-time (or idempotent) fix: infra kustomization uses helmCharts; Argo CD must run
# kustomize build --enable-helm. New installs: deploy/scripts/setup-server.sh does this.
# Existing clusters: run this after upgrading Application manifests that dropped
# spec.source.kustomize.buildOptions.
set -euo pipefail
kubectl patch configmap argocd-cm -n argocd --type merge -p '{"data":{"kustomize.buildOptions":"--enable-helm"}}'
kubectl rollout restart deployment argocd-repo-server -n argocd
echo "argocd-cm updated; argocd-repo-server restarted. Refresh or sync reiver-infra."
