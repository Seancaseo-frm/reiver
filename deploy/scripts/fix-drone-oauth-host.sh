#!/usr/bin/env bash
# Set DRONE_SERVER_HOST to IP:nodePort so GitHub OAuth redirect_uri matches. Run after Drone app has synced once.
# Run from repo root with kubeconfig pointing at your cluster. Uses SERVER_IP from env or deploy/scripts/servers.conf.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVERS_FILE="${SERVERS_FILE:-$SCRIPT_DIR/servers.conf}"
REPO_URL="${REPO_URL:-https://github.com/your-org/reiver.git}"

if [[ -f "$SERVERS_FILE" ]]; then
  read -r first_line < "$SERVERS_FILE" || true
  first_line="${first_line%%#*}"
  first_line="${first_line#"${first_line%%[![:space:]]*}"}"
  [[ -n "$first_line" ]] && SERVER_IP="${SERVER_IP:-${first_line%% *}}"
fi
SERVER_IP="${SERVER_IP:-YOUR_SERVER_IP}"

NODEPORT=$(kubectl get svc -n drone server-drone -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || true)
if [[ -z "$NODEPORT" ]]; then
  echo "Could not get Drone service nodePort (is the Drone app synced?). Create namespace and sync first."
  exit 1
fi

DRONE_HOST_PORT="$SERVER_IP:$NODEPORT"
sed -e "s|REPLACE_REPO_URL|$REPO_URL|g" -e "s|REPLACE_DRONE_HOST|$DRONE_HOST_PORT|g" "$REPO_ROOT/deploy/gitops/argocd/application-drone.yaml" | kubectl apply -f -
echo "Drone Application updated with DRONE_SERVER_HOST=$DRONE_HOST_PORT"
echo "1. Set GitHub OAuth App callback URL to: http://${DRONE_HOST_PORT}/login"
echo "2. Restart server to pick up env: kubectl rollout restart deployment -n drone -l app.kubernetes.io/name=server-drone"
echo "3. Open Drone: http://${DRONE_HOST_PORT}"
