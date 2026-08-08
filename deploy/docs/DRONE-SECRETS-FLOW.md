# Drone pipeline secrets: end-to-end flow and why deployment fails

## Flow (from source code)

1. **Runner** polls the server and receives a stage. It calls `Client.Detail(ctx, stage)` to get execution context.

2. **Server** (Gitness pipeline manager) `Details()`:
   - Fetches stage, execution, repo, pipeline config.
   - Calls `m.Secrets.ListAll(noContext, repo.ParentID)` → gets secrets from DB for the **space** (parent of the repo).
   - Returns `ExecutionContext` with `Secrets: details.Secrets` and converts to `client.Context` with `ConvertToDroneSecrets(details.Secrets)`.

3. **ConvertToDroneSecrets** (gitness `app/pipeline/manager/convert.go`):
   - For each secret it does `ConvertToDroneSecret(s)` → `&drone.Secret{Name: secret.Identifier, Data: secret.Data}`.
   - **No decryption**: it uses `secret.Data` as stored in the DB.

4. **Store**: Secrets are stored **encrypted**. On create, `enc(encrypter, secret)` encrypts `secret.Data` and the ciphertext is what’s persisted. So `ListAll` returns secrets whose `Data` field is **encrypted**.

5. **Runner** receives `client.Context` with `Secrets: []*drone.Secret` where each `Data` is the **encrypted** value. It uses `secret.Static(data.Secrets)` and looks up by name; the value it injects into the clone step is the ciphertext, not the real token. Git then sees an invalid token and fails.

## Root cause

**The pipeline manager returns space secrets to the runner without decrypting them.**

- `Details()` uses `store.SecretStore.ListAll()` → raw DB rows → **encrypted** `Data`.
- It never calls the decryption used elsewhere (e.g. `secretCtrl.Dec(c.encrypter, sec)` in the API controller).
- So the runner always gets encrypted strings; the clone step gets `GIT_PUSH_TOKEN=<ciphertext>`, and GitHub rejects it.

## Encryption key (32 bytes)

In Gitness, the encrypter is configured with **GITNESS_ENCRYPTER_SECRET** (see `types/config.go`). The AES-GCM code in `encrypt/aesgcm.go` does:

```go
func New(key string, compat bool) (Encrypter, error) {
	if len(key) != 32 {
		return nil, errKeySize  // "encryption key must be 32 bytes"
	}
	b := []byte(key)
	block, err := aes.NewCipher(b)
```

So the key must be **exactly 32 bytes** when interpreted as raw bytes (e.g. 32 ASCII characters, or 32 hex chars = 16 bytes if the code hex-decoded, but here it uses `[]byte(key)` so it’s 32 **characters**). The Drone Helm chart exposes this as **DRONE_DATABASE_SECRET**; the image may map that to GITNESS_ENCRYPTER_SECRET or use it only if the server is the older Drone codebase. Either way, a wrong or missing key can also cause the encrypter to not be created (fatal on startup) or to fail when decrypting.

## Fix (in Gitness)

The pipeline manager must **decrypt** secrets before putting them in the execution context:

1. **Inject the encrypter** into the pipeline manager (e.g. in `wire.go` / `ProvideExecutionManager` add `encrypter encrypt.Encrypter`, and pass it into `New`).

2. **In `Details()`**, after `secrets, err := m.Secrets.ListAll(...)`:
   - For each secret, call the same decryption used in the API (e.g. `secretCtrl.Dec(m.encrypter, sec)` or a local helper that uses the encrypter to decrypt `sec.Data`).
   - Put the **decrypted** secrets into `ExecutionContext.Secrets` (so `ConvertToDroneSecrets` receives decrypted data).

Until that change is in the server image you run (e.g. a fixed Gitness/Drone build), the only workaround is to **source secrets outside the server DB** so the runner never receives encrypted blobs:

- Use the **Kubernetes secrets extension**: create a K8s Secret (e.g. `drone-repo-secrets`) with keys `git_push_token`, `registry_username`, `registry_password`, run the `drone/kubernetes-secrets` extension, point the runner at it with `DRONE_SECRET_PLUGIN_ENDPOINT` and `DRONE_SECRET_PLUGIN_TOKEN`, and in `.drone.yml` add the external secret definitions that map those names to the K8s Secret. The runner will then resolve those names via the extension (plaintext) instead of from the server payload.

## References (paths in cloned repos)

- Runner: `runner-go/client/client.go` (Context with Secrets), `runner-go/pipeline/runtime/runner.go` (secrets := secret.Static(data.Secrets)).
- K8s runner: `drone-runner-kube/engine/compiler/compiler.go` (findSecret uses args.Secret and c.Secret), `engine/convert.go` (toSecret uses spec.Secrets).
- Gitness: `app/pipeline/manager/manager.go` (Details, ListAll), `app/pipeline/manager/convert.go` (ConvertToDroneSecrets), `app/api/controller/secret/find.go` (Dec), `encrypt/aesgcm.go` (New key length check).
