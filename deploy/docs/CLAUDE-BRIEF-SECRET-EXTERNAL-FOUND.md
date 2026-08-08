# Brief for Claude: Runner says "found matching secret" but GIT_PUSH_TOKEN still empty

## What we confirmed

1. **`.drone.yml` has top-level `kind: secret` blocks**  
   We have three separate documents at the top:
   - `kind: secret` / `name: git_push_token` / `get.path: drone-repo-secrets` / `get.name: git_push_token`
   - Same for `registry_username` and `registry_password`  
   So the runner should know to call the external plugin for these.

2. **Runner logs show the plugin is called and "finds" the secret**  
   When a pipeline runs we see:
   ```
   secret: database: no matching secret     kind=secret name=git_push_token
   secret: encrypted: no matching secret   kind=secret name=git_push_token
   secret: external: found matching secret  kind=secret name=git_push_token
   ```
   So the runner tries database → encrypted → external, and for `git_push_token` it ends at **external: found matching secret**. Same for `registry_username` and `registry_password`. So the runner **is** calling the plugin and treating the response as a match.

3. **Build still gets empty `GIT_PUSH_TOKEN`**  
   The clone step runs with `x-access-token:@github.com/...` (empty token). So either:
   - The extension returns 200 but with **empty `data`** (e.g. wrong lookup: path/name vs K8s Secret name/key), or
   - The runner gets a non-empty value but **doesn’t inject it** into the step’s environment (bug or wrong field).

4. **Extension logs**  
   With `DEBUG=true` on the extension we still only see "server listening on address :3000". No request logs. So we can’t see from the extension whether it’s receiving requests or what it returns (or the image doesn’t log even with DEBUG).

5. **Direct curl test**  
   We called the extension from inside the cluster with a body `{"name":"git_push_token","path":"drone-repo-secrets","repo":{...}}` and an `Authorization: hmac-sha256 <hex>` header (HMAC-SHA256 of the body with the shared token). Response: **400 Invalid or Missing Signature**. So either the extension expects a different signature scheme (e.g. full HTTP Signatures draft with other headers) or a different header name/format. We didn’t reverse-engineer the exact format; the runner clearly uses something the extension accepts.

6. **K8s Secret**  
   We have Secret `drone-repo-secrets` in namespace `drone` with keys `git_push_token`, `registry_username`, `registry_password`. Docs say **path** = Kubernetes Secret name, **name** = key within that secret. So `path: drone-repo-secrets` and `name: git_push_token` should map to that Secret and key.

## What we need from you

- Given that the runner reports **external: found matching secret** for `git_push_token` but the step still receives an empty value:
  1. In **drone-runner-kube**, when the external plugin returns 200, where does the response body (the `data` field) get written into the build? Is it possible the runner treats "found" as "we got a 200" but then uses an empty or wrong field when building the step’s env/Secret?
  2. In **drone/kubernetes-secrets**, does the extension use `path` as the K8s Secret **name** and `name` as the **key**? If it uses only `name` (e.g. as Secret name) or a different mapping, that would explain empty data even when the Secret exists.
  3. Any way to see the **actual response body** the extension returns for this request (e.g. a different DEBUG flag, or a version that logs the response), or to confirm the exact HTTP Signatures format so we can reproduce the request and capture the response?

## Quick reference

- Runner image: `drone/drone-runner-kube:1.0.0-rc.3`
- Extension image: `drone/kubernetes-secrets:latest`
- Runner has `DRONE_SECRET_PLUGIN_ENDPOINT=http://drone-kubernetes-secrets:3000` and token from `drone-secret-plugin`
- Extension has `SECRET_KEY` from same secret, `KUBERNETES_NAMESPACE=drone`
