#!/usr/bin/env bash
# Clear repo secrets from the Drone/Gitness server database so the pipeline can run
# using only the Kubernetes secrets extension (no UI needed).
# Run from repo root. Requires: kubectl, cluster access, server in namespace drone.
#
# The server fails with "cannot list secrets" / "cipher: message authentication failed"
# when it has repo secrets in the DB. This script deletes those secrets from the DB.
set -e
NAMESPACE="${DRONE_NAMESPACE:-drone}"
SERVER_DEPLOY="server-drone"
PVC_NAME="server-drone"
POD_NAME="drone-clear-secrets-oneoff"

echo "Clearing repo secrets from Drone server DB (namespace=$NAMESPACE)..."

if ! kubectl get deployment -n "$NAMESPACE" "$SERVER_DEPLOY" &>/dev/null; then
  echo "Error: deployment $SERVER_DEPLOY not found in namespace $NAMESPACE"
  exit 1
fi
if ! kubectl get pvc -n "$NAMESPACE" "$PVC_NAME" &>/dev/null; then
  echo "Error: PVC $PVC_NAME not found in namespace $NAMESPACE"
  exit 1
fi

# Scale down server so we can write to SQLite safely
echo "Scaling down server..."
kubectl scale deployment -n "$NAMESPACE" "$SERVER_DEPLOY" --replicas=0
kubectl wait --for=jsonpath='{.status.readyReplicas}'=0 deployment/"$SERVER_DEPLOY" -n "$NAMESPACE" --timeout=120s 2>/dev/null || true
sleep 2

cleanup() {
  echo "Scaling server back up..."
  kubectl scale deployment -n "$NAMESPACE" "$SERVER_DEPLOY" --replicas=1
  kubectl delete pod -n "$NAMESPACE" "$POD_NAME" --force --grace-period=0 2>/dev/null || true
}
trap cleanup EXIT

# One-off pod with sqlite, mount server PVC
kubectl run "$POD_NAME" -n "$NAMESPACE" --image=alpine:3.19 --restart=Never -- \
  sh -c "apk add --no-cache sqlite >/dev/null 2>&1 && sleep 3600"
kubectl wait --for=condition=Ready pod/"$POD_NAME" -n "$NAMESPACE" --timeout=60s
kubectl patch pod "$POD_NAME" -n "$NAMESPACE" -p '{"spec":{"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"'"$PVC_NAME"'"}}],"containers":[{"name":"'"$POD_NAME"'","volumeMounts":[{"name":"data","mountPath":"/data"}]}]}}' 2>/dev/null || true

# Wait for volume mount (patch may not apply to running pod; create with volume from start next)
# So create the pod with the volume from the beginning:
kubectl delete pod "$POD_NAME" -n "$NAMESPACE" --force --grace-period=0 2>/dev/null || true
sleep 2
cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: $POD_NAME
spec:
  containers:
  - name: run
    image: alpine:3.19
    command: ["sh", "-c", "apk add --no-cache sqlite && sleep 3600"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $PVC_NAME
  restartPolicy: Never
EOF
kubectl wait --for=condition=Ready pod/"$POD_NAME" -n "$NAMESPACE" --timeout=90s

# Find DB and delete secrets (Drone: secrets table; some setups: pipeline_secrets)
FOUND=
for DB in /data/database.sqlite /data/gitness.sqlite $(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- find /data -maxdepth 2 -name '*.sqlite' -o -name '*.db' 2>/dev/null | tr -d '\r'); do
  [ -z "$DB" ] && continue
  if kubectl exec -n "$NAMESPACE" "$POD_NAME" -- test -f "$DB" 2>/dev/null; then
    echo "Using database: $DB"
    kubectl exec -n "$NAMESPACE" "$POD_NAME" -- sqlite3 "$DB" ".tables" 2>/dev/null || true
    if kubectl exec -n "$NAMESPACE" "$POD_NAME" -- sqlite3 "$DB" "SELECT COUNT(*) FROM secrets" 2>/dev/null; then
      kubectl exec -n "$NAMESPACE" "$POD_NAME" -- sqlite3 "$DB" "DELETE FROM secrets"
      echo "Deleted all rows from secrets table."
      FOUND=1
    fi
    if kubectl exec -n "$NAMESPACE" "$POD_NAME" -- sqlite3 "$DB" "SELECT 1 FROM pipeline_secrets LIMIT 1" 2>/dev/null; then
      kubectl exec -n "$NAMESPACE" "$POD_NAME" -- sqlite3 "$DB" "DELETE FROM pipeline_secrets"
      echo "Deleted all rows from pipeline_secrets table."
      FOUND=1
    fi
    [ -n "$FOUND" ] && break
  fi
done
[ -z "$FOUND" ] && echo "Warning: no secrets or pipeline_secrets table found; DB schema may differ."

echo "Done. Server is scaling back up. Re-run a build; it should use secrets from the K8s extension only."
