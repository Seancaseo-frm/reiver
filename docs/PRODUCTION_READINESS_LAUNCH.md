# Prompt Hub + Gateway: Production Readiness Assessment

**Assessment date:** June 6, 2026
**Previous assessment:** May 30, 2026
**Scope:** Prompt Hub and Gateway only

**Verdict: Ready for production.** All P0 and P1 blockers from the May 30 assessment have been resolved. The gateway handles routing, provider failover, circuit breakers, caching, key rotation, and observability. The prompt hub supports storage, versioning, rollout traffic splitting, LLM-as-judge quality evaluation, and template injection with fail-closed error handling.

---

## Overall Ratings

| Domain | Score | Summary |
|--------|-------|---------|
| **Gateway Core** | 4.7/5 | Mature retry/fallback, circuit breakers, caching, timeouts, key rotation |
| **Prompt Hub** | 4.5/5 | Full versioning, rollout traffic splitting, LLM-as-judge, fail-closed templates |
| **Ops / Monitoring** | 3.8/5 | Tracing, logging, Kafka readiness, HPA, PDB; still no deploy-level alert rules |
| **Security** | 4.5/5 | Key rotation, CORS restricted, CSP/HSTS, API key scopes, NetworkPolicy |

---

## Resolved Since May 30

| Item | Status | Notes |
|------|--------|-------|
| Rollout traffic splitting (was P0) | Fixed | `resolve_explicit_config` reads rollout weights, assigns variants |
| CORS hardcoded to `allow_origin(Any)` | Fixed | Uses `CORS_ALLOWED_ORIGINS` env var; production set to `https://reiver.ai` |
| No HPA | Fixed | Flow 3-9 replicas at 70% CPU; Website 2-4 replicas |
| No PodDisruptionBudget | Fixed | `minAvailable: 1` for Flow and Website |
| CSP + security headers | Fixed | CSP, HSTS, X-Content-Type-Options, X-Frame-Options, Referrer-Policy |
| Verify Flow network isolation | Fixed | NetworkPolicy restricts ingress to website/watch/mcp/herd; TRUSTED_PROXY_CIDRS enforced |
| Add Kafka to readiness probe | Fixed | `/ready` checks Postgres, Redis, ClickHouse, and Kafka |
| Enforce API key scopes on gateway `/chat/completions` | Fixed | `llm:write` scope required |
| Use `RotatingSecretEncryptor` for key rotation | Fixed | All services use rotating encryptor; re-encrypt CLI + runbook |
| Fail closed on prompt template compile errors | Fixed | Returns 422 instead of sending raw `{{var}}` to LLM |
| Add circuit breakers per provider | Fixed | Sliding-window error rate tracking, auto-open/close |
| Link ClickHouse trace IDs to OTel traces | Fixed | Reads from active OTel span context |

---

## Remaining Items (P2 — post-launch)

| Item | Domain | Impact |
|------|--------|--------|
| Add `cargo test` to Drone CI before image push | CI/CD | Tests exist but don't gate deploys; GitHub Actions test workflow is manual-only |
| Add Flow/Gateway alert rules to deploy manifests | Monitoring | Application-level alerting exists (Watch alert_worker) but no infra-level PrometheusRules |

---

## Architecture Highlights

- **4 services** in production: Flow (gateway + workers), Website (auth + UI), Watch (APM), Herd (scheduler)
- **3-shard ClickHouse cluster** across 3 bare-metal nodes with pod anti-affinity
- **HAProxy L4 load balancer** on Hetzner VPS fronting the k3s cluster
- **Zero-downtime key rotation** via `RotatingSecretEncryptor` + `ENCRYPTION_KEY_OLD` fallback
- **LLM-as-judge** quality evaluation with per-dimension scores (Relevance, Coherence, Helpfulness)
- **Rollout stages** with baseline/target variant assignment and configurable weights
