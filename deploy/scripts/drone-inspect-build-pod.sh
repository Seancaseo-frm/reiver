#!/usr/bin/env bash
# Run this while a Drone build is running to capture the build pod spec and secret.
# Usage: ./deploy/scripts/drone-inspect-build-pod.sh
# Then share the output to see if GIT_PUSH_TOKEN env ref exists and if the secret has data.
set -e
NS="${DRONE_BUILD_NAMESPACE:-drone-builds}"
echo "Waiting for a pod in namespace $NS (trigger a build if none)..."
for i in $(seq 1 24); do
  POD=$(kubectl get pods -n "$NS" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  if [ -n "$POD" ]; then
    echo "=== Pod: $POD ==="
    echo "=== Clone container (first) env vars with secretKeyRef ==="
    kubectl get pod "$POD" -n "$NS" -o json | jq '.spec.containers[0].env[]? | select(.valueFrom.secretKeyRef != null) | {name: .name, secretName: .valueFrom.secretKeyRef.name, key: .valueFrom.secretKeyRef.key}'
    echo "=== All env var names in clone container ==="
    kubectl get pod "$POD" -n "$NS" -o json | jq -r '.spec.containers[0].env[]? | .name' | sort
    echo "=== Secret $POD keys (lengths only) ==="
    kubectl get secret "$POD" -n "$NS" -o json | jq -r '.data | to_entries[] | "\(.key): \(.value | length) chars"'
    exit 0
  fi
  sleep 5
done
echo "No pod found in 2 min. Trigger a build and re-run."
exit 1
