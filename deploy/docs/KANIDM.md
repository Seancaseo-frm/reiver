# Kanidm (identity management + SSH access)

Kanidm runs in the k3s cluster as part of the `reiver-infra` namespace and provides centralized SSH key management for bare metal servers. New team members get a Kanidm account, upload their SSH public key, and can SSH into any configured node.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  k3s cluster (reiver-infra namespace)             │
│                                                      │
│  ┌────────────────┐   ┌───────────────┐              │
│  │ Kanidm server  │───│ PVC (data)    │              │
│  │ :8443 (HTTPS)  │   └───────────────┘              │
│  └───────┬────────┘                                  │
│          │ NodePort :30443                            │
└──────────┼───────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│  Bare metal node                                     │
│                                                      │
│  kanidm_unixd ──► resolves users + caches SSH keys   │
│  sshd ──► AuthorizedKeysCommand → kanidm_unixd       │
└──────────────────────────────────────────────────────┘
```

## Manifests

All Kubernetes resources live in [`deploy/gitops/infra/kanidm/`](../gitops/infra/kanidm/):

| File | Purpose |
|------|---------|
| `kustomization.yaml` | Kustomize entry point |
| `deployment.yaml` | Kanidm server Deployment (image `kanidm/server`) |
| `service.yaml` | NodePort Service on 30443 |
| `pvc.yaml` | 1 Gi PersistentVolumeClaim for `/data` |
| `configmap.yaml` | `server.toml` configuration |
| `certificate.yaml` | cert-manager Certificate for TLS |

The infra Argo CD Application (`reiver-infra`) syncs these automatically.

## Before you deploy

1. **Set your domain.** Edit `deploy/gitops/infra/kanidm/configmap.yaml` and `certificate.yaml` — replace `idm.reiver.ai` with your actual domain or the server IP.

2. **TLS issuer.** The Certificate references a `ClusterIssuer` named `letsencrypt-prod`. If you use a different issuer (or self-signed certs), update `certificate.yaml` accordingly. For self-signed certs, export the CA to use with `kanidm_unixd` clients later.

3. **Commit and push.** Argo CD will deploy Kanidm into `reiver-infra`.

## Initial admin setup (one-time)

Kanidm has two built-in accounts: `admin` (server config) and `idm_admin` (user/group management).
All person, group, and SSH key commands use `idm_admin`.

After the Kanidm pod is running:

```bash
# 1. Recover the auto-generated passwords for both accounts
kubectl exec -n reiver-infra deployment/kanidm -- kanidmd recover-account admin
kubectl exec -n reiver-infra deployment/kanidm -- kanidmd recover-account idm_admin
# Save both passwords somewhere safe.

# 2. Install the kanidm CLI on your laptop:
#   brew tap kanidm/kanidm && brew install kanidm
# Then configure it to point at your server (NodePort):
#   printf 'uri = "https://<node-ip>:30443"\nverify_ca = false\n' > ~/.config/kanidm
# (set verify_ca = true and remove it once you switch to a real TLS cert)

# 3. Login as idm_admin
kanidm login --name idm_admin

# 4. Create a POSIX group for SSH access
kanidm group create ssh_users --name idm_admin
kanidm group posix set --name idm_admin ssh_users
```

## Onboarding a new team member

```bash
# 1. Create their account
kanidm person create <username> "<Full Name>" --name idm_admin

# 2. Enable POSIX attributes (gives them a Linux UID/GID)
kanidm person posix set --name idm_admin <username> --shell /bin/bash

# 3. Add to SSH access group
kanidm group add-members ssh_users <username> --name idm_admin

# 4. Upload their SSH public key (idm_admin can do this, or the user self-services)
kanidm person ssh add-publickey --name idm_admin <username> 'laptop' "ssh-ed25519 AAAA..."
```

Users can also self-manage their SSH keys after logging in:

```bash
kanidm login --name <username>
kanidm person ssh add-publickey --name <username> <username> 'my-key' "$(cat ~/.ssh/id_ed25519.pub)"
```

## Configuring bare metal nodes

Run the setup script on each server that should accept Kanidm-managed SSH logins:

```bash
# SSH into the server, then:
sudo KANIDM_URI=https://<node-ip>:30443 /path/to/deploy/scripts/setup-kanidm-client.sh
```

The script ([`deploy/scripts/setup-kanidm-client.sh`](../scripts/setup-kanidm-client.sh)):

1. Installs `kanidm-unixd-clients`
2. Writes `/etc/kanidm/config` (server URI)
3. Writes `/etc/kanidm/unixd` (PAM login group = `ssh_users`)
4. Updates `/etc/nsswitch.conf` to resolve users via Kanidm
5. Drops `/etc/ssh/sshd_config.d/10-kanidm.conf` for SSH key lookup
6. Enables `kanidm-unixd` and `kanidm-unixd-tasks` systemd services
7. Restarts sshd

Optional env vars:

| Variable | Default | Description |
|----------|---------|-------------|
| `KANIDM_URI` | (required) | Kanidm server URL |
| `KANIDM_CA_PATH` | (none) | Path to CA cert for self-signed TLS |
| `PAM_LOGIN_GROUP` | `ssh_users` | POSIX group allowed to SSH in |

After setup, verify:

```bash
kanidm-unix status
# Should show: system: online / Kanidm: online

kanidm_ssh_authorizedkeys <username>
# Should print the user's SSH public keys
```

## Removing access

```bash
# Remove from SSH group (revokes SSH access on all nodes)
kanidm group remove-members ssh_users <username> --name idm_admin

# Or delete the account entirely
kanidm person delete --name idm_admin <username>
```

The `kanidm_unixd` cache on each node expires stale entries automatically. For immediate revocation, restart `kanidm-unixd` on the target node.

## Troubleshooting

**kanidm-unix status shows "Kanidm: offline"**

- Check that the Kanidm pod is running: `kubectl get pods -n reiver-infra -l app.kubernetes.io/name=kanidm`
- Check that the NodePort is reachable: `curl -k https://<node-ip>:30443`
- Check `/etc/kanidm/config` has the correct `uri`
- If using self-signed certs, ensure `ca_path` is set in `/etc/kanidm/config`

**SSH key not working for a user**

- Verify the user has POSIX attributes: `kanidm person get --name idm_admin <username>` (look for `gidnumber`)
- Verify the user is in `ssh_users`: `kanidm group list-members ssh_users --name idm_admin`
- Verify keys are uploaded: `kanidm person ssh list-publickeys --name idm_admin <username>`
- Test locally on the node: `kanidm_ssh_authorizedkeys <username>`

**Users can resolve but cannot log in**

- Check that `pam_allowed_login_groups` in `/etc/kanidm/unixd` includes the correct group
- Check PAM configuration: `pamtester login <username> authenticate`
