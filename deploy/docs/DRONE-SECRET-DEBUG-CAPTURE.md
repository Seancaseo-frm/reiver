# Drone secret debug proxy – capture summary

## What we captured (from proxy logs)

- **Runner sends:** `path` and `name` with **hyphens** (e.g. `"path":"git-push-token","name":"git-push-token"`).
- **Extension returns:** HTTP 200 with non-empty `data` for all three secrets:
  - `git-push-token`: data length 40
  - `registry-username`: data length 19
  - `registry-password`: data length 40

So the extension is returning secrets correctly and the runner is requesting the right names. The official response format is `{"name":"...","data":"..."}` (see [Drone secret extension docs](https://docs.drone.io/extensions/secret)).

## Conclusion

- **If the clone step still fails** with empty `GIT_PUSH_TOKEN` (e.g. `x-access-token:@...`), the issue is likely in the **runner**: it may not be injecting the extension response `data` into the step environment even when it receives 200 + JSON.
- **If the build succeeds**, the current setup (hyphenated names, separate K8s Secrets, proxy forwarding) is correct and no further change is needed.

## Next step

Run a build and check whether the clone step gets the token. If it still fails, the next place to look is the runner code (e.g. `drone-runner-kube`) and how it uses the secret extension response.
