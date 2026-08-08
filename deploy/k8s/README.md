# Kubernetes Deployment (Reiver)

This directory contains **Kustomize** manifests to run Reiver on Kubernetes. Kubernetes is the **only** supported deployment path; Nomad is no longer used.

## Architecture

- **Namespace**: `reiver`
- **Workloads**: 8 (5 API services + 3 worker deployments) from 5 container images
- **Service discovery**: in-cluster DNS. The website backend uses:
  - `http://reiver-watch:3000`
  - `http://reiver-flow:3001`
  - `http://reiver-pond:3002`
  - `http://reiver-mcp:3002`

| Workload | Image | Port(s) | Replicas |
|----------|-------|--------|----------|
| reiver-watch | reiver-watch | 3000 | 2 |
| reiver-watch-workers | reiver-watch | — | 1 |
| reiver-flow | reiver-flow | 3001 | 2 |
| reiver-pond | reiver-pond | 3002, 5433 (PgWire) | 2 |
| reiver-pond-workers | reiver-pond | — | 1 |
| reiver-website | reiver-website | 3003 | 2 |
| reiver-website-workers | reiver-website | — | 1 |
| reiver-mcp | reiver-mcp | 3002 | 1 |

Services: `reiver-watch`, `reiver-flow`, `reiver-pond`, `reiver-pond-pgwire`, `reiver-website`, `reiver-mcp`.

## Prerequisites

1. **Kubernetes cluster** (e.g. on Hetzner, GKE, EKS) with `kubectl` access
2. **External dependencies** (not in this repo): PostgreSQL, ClickHouse, Redis, Kafka (e.g. Redpanda), and for Pond an S3-compatible store (e.g. R2/MinIO)
3. **Container images** pushed to a registry (e.g. `ghcr.io/owner/reiver-watch`, etc.)
4. **Secrets** created in the `reiver` namespace (see [SECRETS.md](./SECRETS.md)); they are not stored in Git

## Directory layout

- **base/** — shared manifests (Namespace, Deployments, Services). Images use placeholders (`reiver-watch:latest`, etc.).
- **overlays/dev** — dev image tags (e.g. `dev-latest`)
- **overlays/production** — production image tags (e.g. `latest` or version tags)

Before using overlays, replace `REPLACE_OWNER` in the overlay `kustomization.yaml` with your registry owner (e.g. `myorg` for `ghcr.io/myorg/reiver-watch`).

## Creating secrets

Secrets are required before the first deploy. See **[SECRETS.md](./SECRETS.md)** for exact keys and `kubectl create secret generic` examples. Required secrets:

- `reiver-watch` — DB, ClickHouse, Redis, Kafka, JWT, encryption key
- `reiver-flow` — same as watch
- `reiver-pond` — same + R2/S3 bucket and keys
- `reiver-website` — DB, ClickHouse, Redis, JWT, encryption key + `WATCH_URL`, `FLOW_URL`, `POND_URL` (in-cluster URLs above)
- `reiver-mcp` — **no Secret required**. Service URLs (`WEBSITE_URL`, `FLOW_URL`, `WATCH_URL`) are set directly in the Deployment manifest. Authentication is handled per-request by the website proxy.

## Deploy

1. Create secrets (see above).
2. Build and push images to your registry (or use CI).
3. Set image registry in overlay: edit `overlays/production/kustomization.yaml` (or `overlays/dev`) and replace `REPLACE_OWNER` with your registry owner.
4. Apply with Kustomize:

   **Production:**

   ```bash
   kubectl apply -k deploy/k8s/overlays/production
   ```

   **Dev:**

   ```bash
   kubectl apply -k deploy/k8s/overlays/dev
   ```

5. Check rollout:

   ```bash
   kubectl -n reiver get pods,svc
   kubectl -n reiver rollout status deployment/reiver-watch
   # repeat for other deployments
   ```

## Updating images

- **By tag**: change `newTag` in the overlay `kustomization.yaml` (e.g. to `v1.2.3`) and run `kubectl apply -k ...` again. Restart happens via image pull / rollout.
- **By CI**: the deploy workflow can pass the built image tag and run `kubectl set image ...` or regenerate the overlay with the new tag and apply.

## Exposing the API (optional)

The website is the main entrypoint (port 3003). To expose it:

- **LoadBalancer**: set `type: LoadBalancer` on the `reiver-website` Service, or create a separate Service with that type.
- **Ingress**: add an Ingress resource (e.g. in an overlay) that routes host/path to `reiver-website:3003`. TLS and host names are environment-specific.

Example minimal Ingress (add to an overlay and adjust host/tls):

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: reiver-website
  namespace: reiver
spec:
  ingressClassName: nginx
  rules:
    - host: app.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: reiver-website
                port:
                  number: 3003
```

## Troubleshooting

- **Pods not starting**: check `kubectl -n reiver describe pod <pod>` and logs. Often missing or invalid Secret keys (see SECRETS.md).
- **Website can’t reach Watch/Flow/Pond**: ensure `WATCH_URL`, `FLOW_URL`, `POND_URL` in `reiver-website` Secret are exactly `http://reiver-watch:3000`, `http://reiver-flow:3001`, `http://reiver-pond:3002` (same namespace).
- **ImagePullBackOff**: fix image name/tag in the overlay and ensure the registry is pullable from the cluster (imagePullSecrets if private).
