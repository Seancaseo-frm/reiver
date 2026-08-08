# Reply to Claude: Secret resolution and clone token

## Checks you asked for

**Step 1 — Server image**
```text
drone/drone:2.26.0
```

**Step 2 — Repo trusted**  
Not run yet (would need Drone CLI and `drone repo info your-org/reiver`). Can do if you still need it.

**Step 3 — Server debug and logs**  
Server already has `DRONE_LOGS_DEBUG=true`. We grepped recent server logs for secret/endpoint/extension:

```bash
kubectl logs -n drone deployment/server-drone --tail=200 | grep -iE "secret|endpoint|extension"
```
Result: **no matches**. So with debug on, the server is not logging any secret endpoint or extension calls. That suggests the server is **not** calling the secret extension when we run a pipeline.

**Step 4 — GitHub credentials on server**  
Server uses `envFrom: drone-server-secrets`, which contains:
- `DRONE_GITHUB_CLIENT_ID`
- `DRONE_GITHUB_CLIENT_SECRET`
- `DRONE_DATABASE_SECRET`
- `DRONE_COOKIE_SECRET`

So GitHub OAuth is configured. The deployment also has `DRONE_GITHUB=true`.

---

## One important detail about this pipeline

The empty clone token in our case **is** coming from `from_secret`, not from Drone’s built-in git credentials.

Our `.drone.yml` has:

- `clone: disable: true`
- A **manual** step named `clone` that runs `git clone` using `GIT_PUSH_TOKEN`
- That step’s env is explicitly:

  ```yaml
  environment:
    GIT_PUSH_TOKEN:
      from_secret: git_push_token
  ```

So the token used for git is **only** from the secret named `git_push_token`. We are not using Drone’s automatic clone or its GitHub-based token. When the build runs, `GIT_PUSH_TOKEN` is empty, so secret resolution for `git_push_token` is returning nothing.

---

## Summary for you

- Server: **drone/drone:2.26.0**, has `DRONE_SECRET_ENDPOINT` and `DRONE_SECRET_SECRET` set, debug on, GitHub OAuth present.
- Server logs: **no** lines about secret / endpoint / extension → looks like the server is **not** calling the secret extension.
- Clone token: explicitly from `from_secret: git_push_token`; when that resolves empty we get `x-access-token:@github.com/...`.

So we need to understand: for **drone/drone:2.26.0**, when does the server actually call `DRONE_SECRET_ENDPOINT` (e.g. trusted repo only, or different trigger), and should we rely on server-side resolution here or on the runner’s `DRONE_SECRET_PLUGIN_ENDPOINT` and avoid having both set?
