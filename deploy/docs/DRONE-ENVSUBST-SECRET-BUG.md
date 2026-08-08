# Why Drone Kubernetes Runner Silently Empties Your Secrets

## The Symptom

A Drone CI pipeline on a Kubernetes runner has secrets configured via
`from_secret` and an external Kubernetes Secrets extension. The secrets are
resolved successfully — the runner logs confirm it, the K8s Secret it creates
in the build namespace contains the correct values, and the pod spec references
them with the correct `secretKeyRef`. The env var even _exists_ inside the
container (`env | grep -c GIT_PUSH_TOKEN` returns `1`).

Yet every command that uses the variable sees an empty string.

```
DEBUG token_len=0
git ls-remote "https://x-access-token:@github.com/..." HEAD
fatal: Authentication failed
```

## The Components

```
┌──────────────┐      .drone.yml       ┌────────────────────┐
│  Drone       │ ───────────────────▶  │  drone-runner-kube  │
│  Server      │   webhook / poll      │  (Kube Runner)      │
└──────────────┘                       └────────┬───────────┘
                                                │
                          1. envsubst.Eval()     │  2. Compile pipeline
                             on raw YAML         │     (resolve secrets,
                             ──────────────▶     │      generate script)
                                                │
                          3. Create K8s Secret   │  4. Create Pod
                             in build namespace  │     (secretKeyRef)
                                                ▼
                                       ┌────────────────────┐
                                       │  Build Pod          │
                                       │  (drone-builds ns)  │
                                       └────────────────────┘
```

**drone-runner-go** — shared library used by all Drone runners. Contains the
`runtime.Runner.Run()` method that orchestrates pipeline execution.

**drone/envsubst** — a Go library that implements bash-style string
substitution (`${VAR}`, `${VAR:-default}`, `${#VAR}`, etc.).

**Kubernetes Secrets Extension** — a sidecar that the runner queries over HTTP
to resolve `from_secret` references against K8s Secrets in the cluster.

## The Root Cause

In `drone-runner-go/pipeline/runtime/runner.go`, the very first thing the
runner does with the pipeline configuration is run it through `envsubst`:

```go
envs := environ.Combine(
    s.Environ,
    environ.System(data.System),
    environ.Repo(data.Repo),
    environ.Build(data.Build),
    environ.Stage(stage),
    environ.Link(data.Repo, data.Build, data.System),
    data.Build.Params,
)

subf := func(k string) string {
    v := envs[k]
    if strings.Contains(v, "\n") {
        v = fmt.Sprintf("%q", v)
    }
    return v
}

config, err := envsubst.Eval(string(data.Config.Data), subf)
```

This processes the **entire raw `.drone.yml`** — including every shell command
inside `commands:` blocks — before the YAML is even parsed.

The substitution function `subf` looks up variable names in `envs`, which
contains build metadata: `DRONE_BRANCH`, `DRONE_COMMIT_SHA`,
`DRONE_REPO_NAME`, etc. It does **not** contain secret values. Secrets are
resolved later, during the compilation phase.

So when `envsubst` encounters `${GIT_PUSH_TOKEN}` inside a command string, it
calls `subf("GIT_PUSH_TOKEN")`, which does a map lookup in `envs`. Go returns
the zero value for a missing key — an empty string. `envsubst` dutifully
replaces `${GIT_PUSH_TOKEN}` with nothing.

By the time the compiler resolves secrets from the external plugin, creates the
K8s Secret, and wires up `secretKeyRef` in the pod spec — the damage is already
done. The generated shell script (`DRONE_SCRIPT` env var) has the empty values
baked in:

```bash
# What the runner generated (DRONE_SCRIPT):
git ls-remote "https://x-access-token:@github.com/org/repo.git" HEAD

# What it should have been:
git ls-remote "https://x-access-token:${GIT_PUSH_TOKEN}@github.com/org/repo.git" HEAD
```

The env var `GIT_PUSH_TOKEN` is correctly set in the container — you can verify
with `kubectl exec` or by reading `/proc/1/environ` — but the script never
references it. The variable references were erased before the script was built.

## Why It's Hard to Debug

1. **The runner logs say secrets are found.** `"secret: external: found
   matching secret"` appears for every secret. Nothing looks wrong.

2. **The K8s Secret is correct.** Inspecting the ephemeral secret in the build
   namespace shows all keys with correct values and lengths.

3. **The pod spec is correct.** Each env var has a proper `secretKeyRef`
   pointing to the right secret and key, with `optional: true`.

4. **The env var exists in the container.** `env | grep -c GIT_PUSH_TOKEN`
   returns `1`. `printenv GIT_PUSH_TOKEN` returns the full token. Everything
   looks right from inside the container.

5. **But the script ignores it.** The shell script that the runner generates
   and injects as `DRONE_SCRIPT` has already had the variable references
   replaced with empty strings. The shell executes pre-expanded commands that
   contain no `$GIT_PUSH_TOKEN` references at all.

This creates a situation where every layer of the system looks correct in
isolation, but the pipeline still fails.

## The Fix

The `drone/envsubst` library treats `$$` as an escape for a literal `$`. After
envsubst processing, `$$` becomes `$`, preserving the variable reference for
the shell to resolve at runtime.

```yaml
# Before (broken): envsubst expands ${GIT_PUSH_TOKEN} to ""
commands:
  - git clone "https://x-access-token:${GIT_PUSH_TOKEN}@github.com/org/repo.git" .

# After (working): $$ survives envsubst, shell expands $GIT_PUSH_TOKEN at runtime
commands:
  - git clone "https://x-access-token:$${GIT_PUSH_TOKEN}@github.com/org/repo.git" .
```

This applies to any `$VAR` or `${VAR}` reference in `commands:` blocks that
refers to a secret-backed environment variable. Standard Drone variables like
`${DRONE_BRANCH}` do not need escaping because they _are_ in the envsubst
substitution map and expand correctly.

The rule of thumb: **if the variable comes from `from_secret`, escape it with
`$$` in commands.**

## Affected Syntax

All of these are expanded by `drone/envsubst` and need `$$` escaping when
referencing secrets:

| Syntax | envsubst behavior | Fix |
|---|---|---|
| `${SECRET}` | Replaced with `""` | `$${SECRET}` |
| `$SECRET` | Replaced with `""` | `$$SECRET` |
| `${#SECRET}` | Replaced with `0` (length of `""`) | `$${#SECRET}` |
| `${SECRET:-fallback}` | Replaced with `"fallback"` | `$${SECRET:-fallback}` |

## Key Takeaway

Drone's `envsubst` preprocessing is a YAML-level text transformation that runs
before any pipeline semantics are applied. It doesn't know about `from_secret`,
`environment:` blocks, or the distinction between build metadata and secret
values. It sees `${ANYTHING}` and expands it using only the build metadata map.
When the key isn't found, it silently produces an empty string — there is no
warning, no error, and no log message.
