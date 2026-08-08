#!/usr/bin/env bash
# Configure a bare metal server to authenticate SSH users via Kanidm.
#
# Installs kanidm-unixd-clients, configures PAM/nsswitch/sshd to resolve
# users and SSH public keys from the Kanidm server, then restarts services.
#
# Prerequisites:
#   - Kanidm server is running and reachable from this host (see deploy/docs/KANIDM.md)
#   - Ubuntu/Debian host (adjust package install for other distros)
#
# Usage:
#   KANIDM_URI=https://<node-ip>:30443 ./deploy/scripts/setup-kanidm-client.sh
#
# Optional env vars:
#   KANIDM_URI          - Kanidm server URL (required)
#   KANIDM_CA_PATH      - Path to CA cert if using self-signed TLS (optional)
#   PAM_LOGIN_GROUP     - POSIX group allowed to login via PAM (default: ssh_users)
set -euo pipefail

# --- Config ---
KANIDM_URI="${KANIDM_URI:?Set KANIDM_URI to the Kanidm server URL (e.g. https://10.0.0.1:30443)}"
KANIDM_CA_PATH="${KANIDM_CA_PATH:-}"
PAM_LOGIN_GROUP="${PAM_LOGIN_GROUP:-ssh_users}"

echo "==> Kanidm client setup"
echo "    Server: $KANIDM_URI"
echo "    PAM login group: $PAM_LOGIN_GROUP"
echo ""

# --- Detect distro and install packages ---
install_packages() {
  if command -v apt-get &>/dev/null; then
    echo "==> Installing kanidm-unixd-clients (apt)..."
    apt-get update -qq
    apt-get install -y -qq kanidm-unixd-clients
  elif command -v dnf &>/dev/null; then
    echo "==> Installing kanidm-unixd-clients (dnf)..."
    dnf install -y kanidm-unixd-clients
  elif command -v zypper &>/dev/null; then
    echo "==> Installing kanidm-unixd-clients (zypper)..."
    zypper install -y kanidm-unixd-clients
  else
    echo "ERROR: Unsupported package manager. Install kanidm-unixd-clients manually."
    exit 1
  fi
}

install_packages

# --- Write /etc/kanidm/config (client connection) ---
echo "==> Writing /etc/kanidm/config..."
mkdir -p /etc/kanidm

cat > /etc/kanidm/config <<EOF
uri = "${KANIDM_URI}"
EOF

if [[ -n "$KANIDM_CA_PATH" ]]; then
  echo "ca_path = \"${KANIDM_CA_PATH}\"" >> /etc/kanidm/config
fi

# --- Write /etc/kanidm/unixd (UNIX daemon config) ---
echo "==> Writing /etc/kanidm/unixd..."
cat > /etc/kanidm/unixd <<EOF
version = "2"

default_shell = "/bin/bash"
home_prefix = "/home/"
home_attr = "uuid"
home_alias = "name"

[kanidm]
pam_allowed_login_groups = ["${PAM_LOGIN_GROUP}"]
EOF

# --- Update /etc/nsswitch.conf ---
echo "==> Updating /etc/nsswitch.conf..."
if ! grep -q 'kanidm' /etc/nsswitch.conf; then
  cp /etc/nsswitch.conf /etc/nsswitch.conf.bak
  sed -i 's/^passwd:.*/passwd: kanidm compat/' /etc/nsswitch.conf
  sed -i 's/^group:.*/group: kanidm compat/' /etc/nsswitch.conf
  echo "    nsswitch.conf updated (backup at /etc/nsswitch.conf.bak)"
else
  echo "    nsswitch.conf already has kanidm, skipping"
fi

# --- Configure sshd ---
echo "==> Configuring sshd for Kanidm SSH key lookup..."
SSHD_CONF_DIR="/etc/ssh/sshd_config.d"
SSHD_KANIDM_CONF="${SSHD_CONF_DIR}/10-kanidm.conf"

if [[ -d "$SSHD_CONF_DIR" ]]; then
  cat > "$SSHD_KANIDM_CONF" <<'EOF'
PubkeyAuthentication yes
UsePAM yes
AuthorizedKeysCommand /usr/sbin/kanidm_ssh_authorizedkeys %u
AuthorizedKeysCommandUser nobody
PasswordAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
GSSAPIAuthentication no
KerberosAuthentication no
EOF
  echo "    Wrote $SSHD_KANIDM_CONF"
else
  # Fallback: append to main sshd_config if drop-in dir does not exist
  SSHD_CONF="/etc/ssh/sshd_config"
  if ! grep -q 'kanidm_ssh_authorizedkeys' "$SSHD_CONF"; then
    cp "$SSHD_CONF" "${SSHD_CONF}.bak"
    cat >> "$SSHD_CONF" <<'EOF'

# Kanidm SSH key authentication
PubkeyAuthentication yes
UsePAM yes
AuthorizedKeysCommand /usr/sbin/kanidm_ssh_authorizedkeys %u
AuthorizedKeysCommandUser nobody
PasswordAuthentication no
PermitRootLogin no
PermitEmptyPasswords no
GSSAPIAuthentication no
KerberosAuthentication no
EOF
    echo "    Updated $SSHD_CONF (backup at ${SSHD_CONF}.bak)"
  else
    echo "    sshd_config already has kanidm_ssh_authorizedkeys, skipping"
  fi
fi

# --- Enable and start services ---
echo "==> Enabling kanidm-unixd services..."
systemctl enable --now kanidm-unixd
systemctl enable --now kanidm-unixd-tasks

echo "==> Restarting sshd..."
systemctl restart sshd

# --- Verify ---
echo ""
echo "==> Verifying kanidm-unixd status..."
kanidm-unix status || true

echo ""
echo "--- Setup complete ---"
echo "Users in the '${PAM_LOGIN_GROUP}' group with SSH public keys in Kanidm"
echo "can now SSH into this server."
echo ""
echo "Test with: kanidm_ssh_authorizedkeys <username>"
echo "Then:      ssh <username>@$(hostname -f || hostname)"
