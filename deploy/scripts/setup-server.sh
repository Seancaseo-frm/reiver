#!/usr/bin/env bash
# Cluster management: bootstrap a new k3s cluster OR join worker nodes to an existing one.
# Run from repo root.
#
# Usage:
#   ./deploy/scripts/setup-server.sh                 # Bootstrap primary from servers.conf (line 1)
#   ./deploy/scripts/setup-server.sh add-node <IP>   # Join a worker node to the existing cluster
#
# SSH auth: tries key-based auth first (~/.ssh/id_ed25519, ssh-agent, etc.).
# Falls back to password auth (sshpass) if SSH_AUTH=password or key auth fails.
#
# Config: set SERVER_IP and SSH_USER via env, or create deploy/scripts/servers.conf:
#   <primary_ip> [username]   # first line = primary (k3s server)
# Example: YOUR_SERVER_IP root
set -e

# --- Config ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVERS_FILE="${SERVERS_FILE:-$SCRIPT_DIR/servers.conf}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-$HOME/.kube/config-reiver}"
SSH_USER="${SSH_USER:-root}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_AUTH="${SSH_AUTH:-}"  # "key", "password", or empty (auto-detect)

# Parse servers.conf for the primary server IP and user
if [[ -f "$SERVERS_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "$line" ]] && continue
    PRIMARY_IP="${line%% *}"
    rest="${line#* }"
    rest="${rest%%#*}"
    rest="${rest#"${rest%%[![:space:]]*}"}"
    rest="${rest%"${rest##*[![:space:]]}"}"
    if [[ -n "$rest" && "$rest" != "$PRIMARY_IP" ]]; then
      PRIMARY_USER="$rest"
    fi
    break
  done < "$SERVERS_FILE"
fi
PRIMARY_IP="${PRIMARY_IP:-YOUR_SERVER_IP}"
PRIMARY_USER="${PRIMARY_USER:-root}"

# --- SSH helpers ---

# Detect whether to use key or password auth for a given host.
# Sets _SSH_CMD array for the target.
setup_ssh() {
  local user="$1" host="$2"

  if [[ "$SSH_AUTH" == "password" ]]; then
    _setup_password_ssh "$user" "$host"
    return
  fi

  # Try key auth first
  if [[ -f "$SSH_KEY" ]] && ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes "${user}@${host}" 'echo OK' &>/dev/null; then
    _SSH_CMD=(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new)
    _SCP_CMD=(scp -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new)
    echo "SSH: using key auth for ${user}@${host}"
    return
  fi

  # Try ssh-agent / default keys
  if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes "${user}@${host}" 'echo OK' &>/dev/null; then
    _SSH_CMD=(ssh -o StrictHostKeyChecking=accept-new)
    _SCP_CMD=(scp -o StrictHostKeyChecking=accept-new)
    echo "SSH: using default key auth for ${user}@${host}"
    return
  fi

  if [[ "$SSH_AUTH" == "key" ]]; then
    echo "SSH key auth failed for ${user}@${host}. Check that your public key is on the server."
    exit 1
  fi

  # Fall back to password
  echo "Key auth failed, falling back to password auth for ${user}@${host}..."
  _setup_password_ssh "$user" "$host"
}

_setup_password_ssh() {
  local user="$1" host="$2"

  if ! command -v sshpass &>/dev/null; then
    echo "SSH key auth failed and sshpass is not installed (needed for password fallback)."
    echo "Either: add your SSH key to the server, or install sshpass (brew install sshpass)."
    exit 1
  fi

  local credentials_file="$SCRIPT_DIR/.server-credentials"
  if [[ -z "${SSHPASS:-}" && -f "$credentials_file" ]]; then
    SSHPASS=$(sed -n 's/^Password:[[:space:]]*//p' "$credentials_file" | head -1 | tr -d '\r\n')
  fi
  if [[ -z "${SSHPASS:-}" ]]; then
    echo -n "Enter password for ${user}@${host}: "
    read -rs SSHPASS
    echo
  fi

  SSHPASS_FILE=$(mktemp)
  printf '%s' "$SSHPASS" > "$SSHPASS_FILE"
  unset SSHPASS
  trap 'rm -f "$SSHPASS_FILE"' EXIT

  local test_out
  test_out=$(sshpass -f "$SSHPASS_FILE" ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o PreferredAuthentications=password -o PubkeyAuthentication=no "${user}@${host}" 'echo OK' 2>&1) || true
  if [[ "$test_out" != "OK" ]]; then
    echo "Password SSH failed for ${user}@${host}: $test_out"
    exit 1
  fi

  _SSH_CMD=(sshpass -f "$SSHPASS_FILE" ssh -o StrictHostKeyChecking=accept-new)
  _SCP_CMD=(sshpass -f "$SSHPASS_FILE" scp -o StrictHostKeyChecking=accept-new)
  echo "SSH: using password auth for ${user}@${host}"
}

ssh_target() {
  "${_SSH_CMD[@]}" "${_CURRENT_USER}@${_CURRENT_HOST}" "$@"
}

scp_from() {
  "${_SCP_CMD[@]}" "${_CURRENT_USER}@${_CURRENT_HOST}:$1" "$2"
}

# --- add-node command ---
add_node() {
  local node_ip="$1"
  local node_user="${2:-$SSH_USER}"

  if [[ -z "$node_ip" ]]; then
    echo "Usage: $0 add-node <IP> [user]"
    exit 1
  fi

  echo "=== Adding worker node ${node_user}@${node_ip} to cluster ==="

  # We need the join token from the primary. Try kubectl first (if we have cluster access),
  # then fall back to SSH into the primary.
  local join_token=""

  # Method 1: read from a privileged pod (works if kubectl is configured)
  if command -v kubectl &>/dev/null && kubectl get nodes &>/dev/null 2>&1; then
    echo "Retrieving join token via kubectl..."
    join_token=$(kubectl apply -f - <<'TOKENPOD' 2>/dev/null && sleep 5 && kubectl exec -n kube-system get-join-token -- cat /host/var/lib/rancher/k3s/server/token 2>/dev/null; kubectl delete pod get-join-token -n kube-system --ignore-not-found 2>/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: get-join-token
  namespace: kube-system
spec:
  tolerations:
  - operator: "Exists"
  priorityClassName: system-node-critical
  restartPolicy: Never
  containers:
  - name: reader
    image: rancher/mirrored-library-traefik:3.6.9
    command: ["sleep", "300"]
    securityContext:
      privileged: true
    volumeMounts:
    - name: host
      mountPath: /host
      readOnly: true
  volumes:
  - name: host
    hostPath:
      path: /
TOKENPOD
    ) || true
    kubectl delete pod get-join-token -n kube-system --ignore-not-found &>/dev/null || true
  fi

  # Method 2: SSH into the primary to read the token
  if [[ -z "$join_token" ]]; then
    echo "Retrieving join token via SSH to primary (${PRIMARY_USER}@${PRIMARY_IP})..."
    _CURRENT_USER="$PRIMARY_USER"
    _CURRENT_HOST="$PRIMARY_IP"
    setup_ssh "$PRIMARY_USER" "$PRIMARY_IP"
    join_token=$(ssh_target 'cat /var/lib/rancher/k3s/server/token 2>/dev/null || cat /var/lib/rancher/k3s/server/node-token 2>/dev/null') || true
  fi

  if [[ -z "$join_token" ]]; then
    echo "Failed to retrieve join token. Provide it manually:"
    echo "  K3S_TOKEN=<token> $0 add-node $node_ip"
    exit 1
  fi

  # Allow override via env
  join_token="${K3S_TOKEN:-$join_token}"

  echo "Join token retrieved."

  # Set up SSH to the new node
  _CURRENT_USER="$node_user"
  _CURRENT_HOST="$node_ip"
  setup_ssh "$node_user" "$node_ip"

  echo "Installing k3s agent on ${node_user}@${node_ip}..."
  ssh_target "curl -sfL https://get.k3s.io | K3S_URL=https://${PRIMARY_IP}:6443 K3S_TOKEN='${join_token}' sh -"

  echo "Waiting for node to join the cluster..."
  for i in {1..30}; do
    if kubectl get nodes 2>/dev/null | grep -q "$node_ip"; then
      break
    fi
    sleep 5
  done

  echo ""
  echo "=== Node status ==="
  kubectl get nodes -o wide 2>/dev/null || echo "(kubectl not available — verify with: kubectl get nodes)"
  echo ""
  echo "Worker node ${node_ip} added successfully."
}

# --- Dispatch: add-node vs full bootstrap ---
if [[ "${1:-}" == "add-node" ]]; then
  add_node "${2:-}" "${3:-}"
  exit 0
fi

# ============================================================================
# Full cluster bootstrap (original setup-server.sh flow)
# ============================================================================

SERVER_IP="${SERVER_IP:-$PRIMARY_IP}"
SSH_USER="${SSH_USER:-$PRIMARY_USER}"

if ! command -v kubectl &>/dev/null; then
  echo "Missing kubectl. Install it and re-run."
  exit 1
fi
if [[ ! -f "$REPO_ROOT/deploy/gitops/argocd/application-infra.yaml" ]]; then
  echo "Run this script from the repo root (or ensure deploy/gitops/argocd/application-infra.yaml exists)."
  exit 1
fi

_CURRENT_USER="$SSH_USER"
_CURRENT_HOST="$SERVER_IP"
setup_ssh "$SSH_USER" "$SERVER_IP"

# --- Step 1: Install k3s on server ---
echo "Installing k3s on server..."
ssh_target 'curl -sfL https://get.k3s.io | sh'

echo "Waiting for k3s to be ready..."
for i in {1..60}; do
  if ssh_target 'test -f /etc/rancher/k3s/k3s.yaml' 2>/dev/null; then
    break
  fi
  sleep 5
done
if ! ssh_target 'test -f /etc/rancher/k3s/k3s.yaml' 2>/dev/null; then
  echo "k3s did not become ready in time."
  exit 1
fi
echo "k3s is ready."

# --- Step 2: Fetch kubeconfig ---
echo "Fetching kubeconfig..."
mkdir -p "$(dirname "$KUBECONFIG_PATH")"
scp_from /etc/rancher/k3s/k3s.yaml "$KUBECONFIG_PATH"
tmp_kube="$(mktemp)"
sed "s|127.0.0.1|$SERVER_IP|g" "$KUBECONFIG_PATH" > "$tmp_kube" && mv "$tmp_kube" "$KUBECONFIG_PATH"

export KUBECONFIG="$KUBECONFIG_PATH"
echo "Kubeconfig saved to $KUBECONFIG_PATH"
echo "To use: export KUBECONFIG=$KUBECONFIG_PATH && kubectl get nodes"

# --- Step 3: Install Argo CD ---
echo "Installing Argo CD..."
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

echo "Waiting for Argo CD pods to be ready..."
for i in {1..60}; do
  not_ready=$(kubectl -n argocd get pods -o jsonpath='{.items[*].status.phase}' 2>/dev/null | tr ' ' '\n' | grep -v Running | grep -v Succeeded | grep -c . || true)
  if [[ "${not_ready:-1}" -eq 0 ]] && kubectl -n argocd get pods --no-headers 2>/dev/null | grep -q Running; then
    break
  fi
  sleep 5
done
echo "Argo CD is up."

if kubectl get configmap argocd-cm -n argocd >/dev/null 2>&1; then
  kubectl patch configmap argocd-cm -n argocd --type merge -p '{"data":{"kustomize.buildOptions":"--enable-helm"}}'
  kubectl rollout restart deployment argocd-repo-server -n argocd 2>/dev/null || true
fi

# --- Step 4: Apply Argo CD Applications ---
REPO_URL_RAW="$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)"
if [[ "$REPO_URL_RAW" =~ ^git@github\.com:(.+)$ ]]; then
  REPO_URL="https://github.com/${BASH_REMATCH[1]}"
elif [[ -z "$REPO_URL_RAW" ]]; then
  REPO_URL=""
else
  REPO_URL="$REPO_URL_RAW"
fi
if [[ -z "$REPO_URL" ]]; then
  echo "Could not get repo URL from 'git remote get-url origin'. Set REPO_URL and apply manually."
  exit 1
fi

echo "Applying Argo CD Applications (repo: $REPO_URL)..."

if [[ -f "$REPO_ROOT/deploy/gitops/argocd/application-argocd-bootstrap.yaml" ]]; then
  sed "s|REPLACE_REPO_URL|$REPO_URL|g" "$REPO_ROOT/deploy/gitops/argocd/application-argocd-bootstrap.yaml" | kubectl apply -f -
  echo "Bootstrap app applied."
fi
if [[ -f "$REPO_ROOT/deploy/gitops/argocd/project-reiver.yaml" ]]; then
  kubectl apply -f "$REPO_ROOT/deploy/gitops/argocd/project-reiver.yaml"
fi
sed "s|REPLACE_REPO_URL|$REPO_URL|g" "$REPO_ROOT/deploy/gitops/argocd/application-infra.yaml" | kubectl apply -f -
sed "s|REPLACE_REPO_URL|$REPO_URL|g" "$REPO_ROOT/deploy/gitops/argocd/application-app-production.yaml" | kubectl apply -f -
if [[ -f "$REPO_ROOT/deploy/gitops/argocd/application-drone.yaml" ]]; then
  sed -e "s|REPLACE_REPO_URL|$REPO_URL|g" -e "s|REPLACE_DRONE_HOST|$SERVER_IP|g" "$REPO_ROOT/deploy/gitops/argocd/application-drone.yaml" | kubectl apply -f -
  echo "Drone CI Application applied."
fi
echo "Applications applied. Argo CD will sync from the repo."

# --- Next steps ---
echo ""
echo "--- Next steps ---"
echo "1. Create the four app Secrets in namespace reiver (DB passwords, JWT, R2, etc.):"
echo "   See deploy/SETUP.md Step 6 and deploy/k8s/SECRETS.md"
echo ""
echo "2. (Optional) Get Argo CD admin password:"
echo "   kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath=\"{.data.password}\" | base64 -d && echo"
echo ""
echo "3. To add worker nodes to this cluster:"
echo "   ./deploy/scripts/setup-server.sh add-node <IP> [user]"
echo ""
echo "4. Use this kubeconfig: export KUBECONFIG=$KUBECONFIG_PATH"
