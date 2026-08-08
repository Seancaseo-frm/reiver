# Verify Drone secret resolution (GIT_PUSH_TOKEN)

## 1. Confirm the build used the right .drone.yml

The pipeline config is taken from **the commit that is being built**. If you trigger a build from an old commit, it will use the old `get.path: drone-repo-secrets` and the extension will return empty.

- In **Drone UI** → open the failing build → **Configuration** tab. Check that the YAML shows:
  ```yaml
  kind: secret
  name: git-push-token
  get:
    path: git-push-token
    name: git-push-token
  ```
  If you still see `path: drone-repo-secrets`, the build was from a commit before the fix. Push the change and run a **new** build from the latest commit.

- Or run: `drone build info your-org/reiver <build-number>` and confirm the `ref`/`sha` is the commit that has the updated .drone.yml.

## 2. Inspect what the runner put in the build pod

While a build is **running** (clone step pending or running), in another terminal:

```bash
./deploy/scripts/drone-inspect-build-pod.sh
```

Check the output:
- **Secret key lengths**: if `GIT_PUSH_TOKEN` shows `0 chars`, the runner injected an empty value.
- If it shows ~40 chars, the token is present and the failure may be elsewhere.

## 3. Extension responds correctly when path is correct

We already verified with a direct HTTP Signatures call: when the request has `path: "git-push-token"` and `name: "git-push-token"`, the extension returns 200 and the token. So the extension works when the **runner sends the right path**. The runner gets `path` and `name` from the **pipeline config** (the .drone.yml of the commit being built). So the commit being built **must** contain `get.path: git-push-token`.

## Checklist

1. [ ] .drone.yml in **main** (or the branch you build) has `get.path: git-push-token` and `get.name: git-push-token` for the git-push-token secret.
2. [ ] You **pushed** that commit (e.g. to main).
3. [ ] You triggered a **new** build **from that commit** (e.g. push to main, or "Run pipeline" and ensure it uses the latest commit).
4. [ ] In the build’s **Configuration** tab you see `path: git-push-token`, not `path: drone-repo-secrets`.

If all are true and GIT_PUSH_TOKEN is still empty, run the inspect script during the build and share the output (key lengths only, no values).
