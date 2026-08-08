# Drone CI (self-hosted)

Deploy pipeline runs from [.drone.yml](../../.drone.yml): build and push the five Docker images to ghcr.io (using the Docker plugin), then update the production Kustomize overlay and push so Argo CD syncs. All of it runs on your server. No “Trusted” or Kaniko—use the standard Docker plugin.

## Where it runs

**Production (this repo):** [setup-server.sh](../scripts/setup-server.sh) deploys Drone into the cluster via Argo CD (see [application-drone.yaml](../gitops/argocd/application-drone.yaml)). Drone server and Kubernetes runner run in the `drone` namespace. UI is at **http://YOUR_SERVER_IP:&lt;port&gt;** (NodePort; get port: `kubectl get svc -n drone server-drone -o jsonpath='{.spec.ports[0].nodePort}'`). Everything stays on your server.

## Setup (after running setup-server.sh)

1. **Create Kubernetes secrets** so Drone can start (chart expects these in namespace `drone`):
   ```bash
   kubectl create namespace drone --dry-run=client -o yaml | kubectl apply -f -
   kubectl create secret generic drone-server-secrets -n drone \
     --from-literal=DRONE_GITHUB_CLIENT_ID="<your-github-oauth-client-id>" \
     --from-literal=DRONE_GITHUB_CLIENT_SECRET="<your-github-oauth-client-secret>" \
     --from-literal=DRONE_COOKIE_SECRET="$(openssl rand -hex 16)" \
     --from-literal=DRONE_DATABASE_SECRET="$(openssl rand -hex 32)"
   kubectl create secret generic drone-rpc-secret -n drone \
     --from-literal=DRONE_RPC_SECRET="$(openssl rand -hex 16)"
   ```
   `DRONE_DATABASE_SECRET` must be **32 bytes** (64 hex characters); use `openssl rand -hex 32`. It is required so the server can encrypt and decrypt repository secrets (e.g. `git_push_token`) and send them to the runner. Set it before adding repo secrets in the Drone UI.
   Then restart the server so it picks them up: `kubectl rollout restart deployment -n drone -l app.kubernetes.io/name=drone` (or restart the drone server deployment by name).

2. **GitHub OAuth App**  
   Create an OAuth App. **Authorization callback URL** must be **`http://<host>:<port>/login`** (path is `/login`, not `/authorize` — the server registers the OAuth callback at `/login`; see [harness/harness](https://github.com/harness/harness) drone branch `handler/web/web.go`). Run once after first sync:
   ```bash
   ./deploy/scripts/fix-drone-oauth-host.sh
   ```
   Then set the callback URL to `http://<that-host-port>/login` (e.g. `http://YOUR_SERVER_IP:30372/login`). Restart the server as the script suggests.

3. **Pipeline secrets (Kubernetes – recommended)**  
   The pipeline resolves `from_secret: git_push_token` (and registry credentials) from a **Kubernetes Secret** via the [Drone Kubernetes secrets extension](https://github.com/drone/drone-kubernetes-secrets). This avoids the Gitness bug where UI repo secrets are sent to the runner encrypted (see [DRONE-SECRETS-FLOW.md](DRONE-SECRETS-FLOW.md)).

   Create two secrets in namespace `drone` (once per cluster):

   **a) Plugin token** (shared between runner and extension so the runner can call the extension):
   ```bash
   kubectl create secret generic drone-secret-plugin -n drone \
     --from-literal=DRONE_SECRET_PLUGIN_TOKEN="$(openssl rand -hex 16)"
   ```

   **b) Repo secrets** (values used by the pipeline; do not commit these).  
   The **drone/kubernetes-secrets** extension uses **`get.path`** as the K8s Secret name and **`get.name`** as the key inside that Secret. Use hyphenated names (K8s disallows underscores). In `.drone.yml` set `get.path` and `get.name` to the K8s Secret name (e.g. `git-push-token`) so the extension looks up the right Secret and key:

   ```bash
   kubectl create secret generic git-push-token -n drone \
     --from-literal=git-push-token="<github-pat-with-repo-scope>"
   kubectl create secret generic registry-username -n drone \
     --from-literal=registry-username="<github-username>"
   kubectl create secret generic registry-password -n drone \
     --from-literal=registry-password="<github-pat-with-write-packages>"
   ```

   You can keep a single `drone-repo-secrets` for your own reference and copy values from it when creating the above (see troubleshooting).

   The extension runs **in the same pod as the runner** (sidecar), per [Drone Kubernetes Secrets docs](https://docs.drone.io/runner/extensions/kube/). Enable with `secretsExtension.enabled: true` in the chart (see [application-drone.yaml](../gitops/argocd/application-drone.yaml)). After the Drone app is synced, create the secrets above and re-run a build. If you see **"cipher: message authentication failed"** in the runner, the chart avoids that by not injecting server secrets into the runner when the extension is enabled; restart runner and server once: `kubectl rollout restart deployment -n drone server-drone runner-drone`.

   **Alternative:** Repo secrets in Drone UI (Settings → Secrets) are supported by Drone, but in Gitness they are currently sent to the runner encrypted (bug). Prefer the Kubernetes secrets above.

4. **Runner and Docker**  
   The Kubernetes runner runs pipeline steps as pods. The Docker plugin uses Docker-in-Docker. If builds fail, the runner may need privileged or a different configuration (see Drone docs).

5. **Rotating the GitHub PAT**  
   The PAT is used in four places across three namespaces. See [GITHUB-TOKEN-ROTATION.md](GITHUB-TOKEN-ROTATION.md) for the full checklist and an all-in-one script.

6. **Repo path**  
   `.drone.yml` uses `ghcr.io/your-org/reiver-*`. If your org/repo differ, edit the `repo:` and overlay `sed` in `.drone.yml`.

## Trigger

Push to `main` or `master`; or run a build from the Drone UI.

## Troubleshooting

**Repository secrets empty in pipeline (e.g. `GIT_PUSH_TOKEN` empty, `x-access-token:@github.com/...`)**

Build runs from a push but `from_secret: git-push-token` (or legacy `git_push_token`) resolves to empty in the clone or update-overlay step.

1. **Set `DRONE_DATABASE_SECRET` on the server**  
   The server needs this to encrypt/decrypt repo secrets and send them to the Kubernetes runner. If it was never set, add it and restart:
   ```bash
   kubectl get secret drone-server-secrets -n drone -o json | jq -r '.data | keys[]'
   # If DRONE_DATABASE_SECRET is missing, patch the secret (replace NEW_HEX with output of: openssl rand -hex 16)
   kubectl patch secret drone-server-secrets -n drone --type='json' -p='[{"op":"add","path":"/data/DRONE_DATABASE_SECRET","value":"'$(echo -n "$(openssl rand -hex 16)" | base64)'"}]'
   kubectl rollout restart deployment -n drone -l app.kubernetes.io/name=server-drone
   ```
   Then in Drone UI → repo → Settings → Secrets, **delete and re-add** `git_push_token`, `registry_username`, and `registry_password` (they must be stored after the server has `DRONE_DATABASE_SECRET`). Re-run the pipeline from a push.

2. **Extension lookup**  
   The extension looks up a K8s Secret **named** like the Drone secret (e.g. `git-push-token`). Create one Secret per value (see step 3.b above). In `.drone.yml` use `from_secret: git-push-token` (hyphens; K8s Secret names cannot contain underscores).

3. **Trigger**  
   "Disabled" under PULL REQUESTS in the secrets list only means they are not available for PR builds. For push builds they should be available; no toggle to "enable" for push.

**Build not starting / "cannot get stage details" / "manager: cannot list secrets" / "cipher: message authentication failed"**

The server fails when it tries to list or decrypt repo secrets before sending the stage to the runner. When you use the **Kubernetes secrets extension** (and have `drone-repo-secrets` in the cluster), the runner gets secrets from the extension only, so the server does not need to send any.

**Fix:** Either remove repo secrets so the server has none to list, or clear them from the database (if the UI fails with 404 on the secrets page):

- **Option A (UI):** In the Drone UI go to your repo → **Settings → Secrets** and delete any repo secrets. Leave the list empty. Then re-run the build.
- **Option B (DB, when UI returns 404 for `/api/repos/.../secrets`):** Run the script that scales down the server, clears the secrets table in the server’s SQLite DB, and scales back up. From repo root:  
  `./deploy/scripts/drone-clear-repo-secrets-db.sh`  
  Then re-run a build; the pipeline will get secrets from the K8s extension only.

If you are not using the extension and see this error, ensure `DRONE_DATABASE_SECRET` in `drone-server-secrets` has not changed since the secrets were added; if it was recreated, delete and re-add all repo secrets in the UI so they are re-encrypted with the current key, then restart the server.

**"Failed to load target state … authentication required: Repository not found"**

Argo CD is trying to clone your repo (where the Drone chart lives) and either can’t find it or isn’t allowed in.

1. **Repo URL not replaced**  
   The Drone Application must be applied with `REPLACE_REPO_URL` replaced by your repo URL. Re-apply from repo root:
   ```bash
   ./deploy/scripts/apply-argocd-apps.sh
   ```
   (Set `REPO_URL` if needed, e.g. `export REPO_URL=https://github.com/your-org/reiver.git`.)

2. **Private repo**  
   If the repo is private, add a repo credential once (from repo root):
   ```bash
   export GITHUB_TOKEN=your_github_pat_with_repo_scope
   ./deploy/scripts/argocd-add-repo-secret.sh
   ```
   Or in Argo CD UI: **Settings → Repositories → Connect repo** (HTTPS + token or SSH key).

**GET /api/user returns 401 / 404 after GitHub redirect**

No session was created after GitHub sends you back to `/authorize?code=...&state=...`. Fixes:

1. **Callback URL must use `/login`** — The Drone server (harness/harness drone branch) registers the OAuth callback at **`/login`**, not `/authorize`. In GitHub OAuth App set **Authorization callback URL** to `http://YOUR_SERVER_IP:<nodePort>/login` (e.g. `http://YOUR_SERVER_IP:30372/login`). Using `/authorize` hits the SPA fallback and never runs the token exchange.

2. **No quotes or spaces in secrets** — When creating `drone-server-secrets`, paste Client ID and Client secret with no extra quotes or trailing spaces. Recreate the secret and restart the server:
   ```bash
   kubectl create secret generic drone-server-secrets -n drone \
     --from-literal=DRONE_GITHUB_CLIENT_ID="<paste-from-github-no-spaces>" \
     --from-literal=DRONE_GITHUB_CLIENT_SECRET="<paste-from-github-no-spaces>" \
     --from-literal=DRONE_COOKIE_SECRET="$(openssl rand -hex 16)" \
     --dry-run=client -o yaml | kubectl apply -f -
   kubectl rollout restart deployment -n drone -l app.kubernetes.io/name=server-drone
   ```

3. **DRONE_SERVER_HOST must include port** — Must be `YOUR_SERVER_IP:<nodePort>`, not just the IP. Run `./deploy/scripts/fix-drone-oauth-host.sh` (after first sync); it re-applies the Application with the correct host:port, then restart the server.

4. **Still 404 in incognito** — The "404" may be the app’s “not logged in” view (SPA loads, then `/api/user` returns 401). Debug:
   - **Server logs:** In one terminal run `kubectl logs -n drone -l app.kubernetes.io/name=server-drone -f --tail=50`. In the browser, click “Login with GitHub” and complete the redirect to `http://<host>:<port>/authorize?code=...`. Watch logs for any `oauth`, `login`, `exchange`, or error lines. If nothing appears for the callback, the request may not be reaching the backend or the backend isn’t handling `/authorize`.
   - **Browser Network tab:** Open DevTools → Network. Repeat login. When you land on `/login?code=...` check: (1) the request to `/login` — status 302 redirect or 200? (2) any request to `/api/user` — status? If `/login` returns 200 with HTML (SPA), you used `/authorize` in GitHub; the server callback is at `/login` only.
   - **Cookie:** In the **200 response** for `GET /authorize`, check **Response Headers** for **Set-Cookie**. If there is no Set-Cookie, the server didn’t create a session (token exchange may have failed). If Set-Cookie is present but the next request still gets 401, the cookie may not be sent (e.g. SameSite). We set `DRONE_COOKIE_SAMESITE=lax` in the Application so the cookie is sent after the cross-site redirect from GitHub; restart the server and try again.

5. **Callback returns 200 with SPA HTML (no session)** — You likely set the GitHub callback to **`/authorize`**. The server (harness/harness drone branch) only registers the OAuth callback at **`/login`**. Change the GitHub OAuth App callback URL to `http://<host>:<port>/login` and try again.
