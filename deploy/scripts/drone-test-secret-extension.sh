#!/usr/bin/env bash
# Test the drone/kubernetes-secrets extension from inside the cluster with the same
# auth the runner uses. Run from repo root. Requires: kubectl, cluster access.
#
# Usage: ./deploy/scripts/drone-test-secret-extension.sh
#
# If the extension returns the secret value, the extension works and the issue
# is in how the runner uses the response. If it returns empty or 401, the
# extension or auth is the problem.
set -e
NAMESPACE="${DRONE_NAMESPACE:-drone}"
SVC="drone-kubernetes-secrets:3000"

echo "Getting shared token from drone-secret-plugin..."
TOKEN=$(kubectl get secret drone-secret-plugin -n "$NAMESPACE" -o jsonpath='{.data.DRONE_SECRET_PLUGIN_TOKEN}' | base64 -d)
[ -n "$TOKEN" ] || { echo "Error: DRONE_SECRET_PLUGIN_TOKEN empty"; exit 1; }

# Request body matching what the runner sends (path = K8s Secret name, name = key)
BODY='{"name":"git_push_token","path":"drone-repo-secrets","repo":{"slug":"your-org/reiver"},"build":{}}'

# HTTP Signatures draft: signature is often HMAC-SHA256 of the body
# The extension may expect header "Authorization: hmac-sha256 <hex>" or similar
SIG=$(echo -n "$BODY" | openssl dgst -sha256 -hmac "$TOKEN" -hex | awk '{print $2}')
echo "Sending POST to $SVC with HMAC signature..."

# Run curl from a pod so it can reach the service
kubectl run curl-secret-test --rm -i --restart=Never -n "$NAMESPACE" --image=curlimages/curl:latest -- \
  curl -s -w "\nHTTP_CODE:%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: hmac-sha256 $SIG" \
  -d "$BODY" \
  "http://$SVC"

echo ""
echo "Check: 200 + JSON with 'data' = extension works. 401/204 or empty data = auth or lookup issue."
