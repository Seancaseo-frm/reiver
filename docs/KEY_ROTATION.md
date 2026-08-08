# Encryption Key Rotation

Zero-downtime procedure for rotating the AES-256-GCM encryption key used to protect secrets at rest (SSO credentials, provider API keys, MFA secrets, etc.).

## How It Works

All services use `RotatingSecretEncryptor` which supports:
- **Encrypt**: always uses the primary key (`ENCRYPTION_KEY`)
- **Decrypt**: tries the primary key first, then falls back to old keys (`ENCRYPTION_KEY_OLD`)

This means you can deploy a new key without any downtime — existing data decrypts via the fallback, and all new writes use the new key.

## Prerequisites

- `kubectl` access to the production cluster
- Ability to run the `re-encrypt-secrets` binary with database access

## Procedure

### 1. Generate a new key

```bash
openssl rand -base64 32
```

Save the output. This is your new primary key.

### 2. Update the Kubernetes secret

```bash
# Get the current ENCRYPTION_KEY value
kubectl get secret app-secrets -o jsonpath='{.data.ENCRYPTION_KEY}' | base64 -d

# Set the OLD key (current key moves to fallback)
kubectl patch secret app-secrets -p '{"stringData": {
  "ENCRYPTION_KEY_OLD": "<current-key-value>",
  "ENCRYPTION_KEY": "<new-key-from-step-1>"
}}'
```

If you already have an `ENCRYPTION_KEY_OLD` value (from a previous rotation that hasn't been cleaned up), append the current primary key to it with a comma:

```bash
ENCRYPTION_KEY_OLD="<current-primary>,<existing-old-keys>"
```

### 3. Rolling restart all services

```bash
kubectl rollout restart deployment/flow deployment/website deployment/watch deployment/pond
kubectl rollout status deployment/flow deployment/website deployment/watch deployment/pond
```

After this step:
- All new encryptions use the new key
- All decryptions try new key first, fall back to old key
- No data is lost or inaccessible

### 4. Re-encrypt existing secrets

Run the re-encryption tool to migrate all stored secrets to the new key:

```bash
# From a pod with database access, or locally with DATABASE_URL set:
DATABASE_URL="postgres://..." \
ENCRYPTION_KEY="<new-key>" \
ENCRYPTION_KEY_OLD="<old-key>" \
re-encrypt-secrets
```

Or with dry-run first to see what would be updated:

```bash
re-encrypt-secrets --dry-run
```

### 5. Verify completion

```bash
re-encrypt-secrets --dry-run
```

The output should show `Rows re-encrypted: 0` for all tables.

### 6. Remove old key

Once verified, remove the fallback key:

```bash
kubectl patch secret app-secrets --type=json -p '[{"op": "remove", "path": "/data/ENCRYPTION_KEY_OLD"}]'
```

### 7. Final rolling restart

```bash
kubectl rollout restart deployment/flow deployment/website deployment/watch deployment/pond
```

This cleans up the fallback key from memory. The rotation is complete.

## Rollback

If something goes wrong after step 3 but before step 4:
- Swap the keys back: set `ENCRYPTION_KEY` to the old key, remove `ENCRYPTION_KEY_OLD`
- Rolling restart

If something goes wrong during step 4 (re-encryption):
- The tool is idempotent. Fix the issue and re-run.
- Data encrypted with either key remains accessible as long as both keys are in the config.

## Multiple Old Keys

`ENCRYPTION_KEY_OLD` supports comma-separated keys for scenarios where multiple rotations happened before re-encryption completed:

```
ENCRYPTION_KEY_OLD=<previous-key>,<even-older-key>
```

Keys are tried in order (left to right) after the primary key fails.

## Tables with Encrypted Data

The re-encryption tool processes these tables/columns:

| Table | Column |
|-------|--------|
| `sso_connections` | `client_secret_encrypted` |
| `sso_connections` | `sp_private_key_encrypted` |
| `sso_connections` | `okta_api_token_encrypted` |
| `deploy_keys` | `private_key_encrypted` |
| `mfa_factors` | `secret_encrypted` |
| `notification_channels` | `api_token_encrypted` |
| `notification_channels` | `client_secret_encrypted` |
| `warehouse_sources` | `secret_access_key_encrypted` |
| `warehouse_sources` | `password_encrypted` |
| `project_settings` | `value` (gateway API keys only) |
| `secret_slots` | `encrypted_value` (filled slots only) |

## Key Generation

Always use cryptographically secure random bytes:

```bash
# Linux/macOS
openssl rand -base64 32

# Alternative
head -c 32 /dev/urandom | base64
```

The key must be exactly 32 bytes (256 bits) before base64 encoding, resulting in a 44-character base64 string.
