# Reiver Feature Design Document

This document outlines the design and rationale for 8 key differentiating features for the Reiver APM platform.

**Document Version:** 1.1  
**Last Updated:** January 2026  
**Status:** Draft (Revised)

---

## Table of Contents

1. [Cost Forecasting & Usage Billing](#1-cost-forecasting--usage-billing)
2. [Notification Settings for Billing Alerts](#2-notification-settings-for-billing-alerts)
3. [AI-Powered Root Cause Analysis](#3-ai-powered-root-cause-analysis)
4. [LLM/AI Agent Observability](#4-llmai-agent-observability)
5. [Datadog Migration Tools](#5-datadog-migration-tools)
6. [Continuous Profiling with Trace Correlation](#6-continuous-profiling-with-trace-correlation)
7. [UX Simplicity](#7-ux-simplicity)
8. [Self-Hosted Licensing](#8-self-hosted-licensing)

---

## 1. Cost Forecasting & Usage Billing

### What It Is

A real-time cost visibility and forecasting system that shows users their current Reiver usage, projected costs for the billing period, and alerts before they exceed budgets.

### Why We're Adding This

**Competitive differentiation**: The #1 complaint about Datadog is surprise bills. Coinbase's infamous $65M Datadog bill made headlines. 67% of organizations report unexpected observability costs. Users want predictable pricing with visibility into their spending.

**Business value**: Cost transparency builds trust and reduces churn. Users who understand their costs are less likely to leave due to bill shock.

### How It Works Within Existing Code

**Data sources**: Reiver already ingests spans, logs, metrics, and LLM requests into ClickHouse. We'll add materialized views that aggregate hourly counts per project, which feed into the forecasting system.

**Integration points**:
- The existing `src/app_state.rs` will hold a reference to a new `BillingService`
- A new background worker (similar to `alert_worker.rs`) runs hourly to snapshot usage
- New API routes mount under `/api/billing`

**Storage architecture** (two-tier aggregation):

```
ClickHouse (real-time)                    PostgreSQL (billing source of truth)
┌────────────────────────┐                ┌────────────────────────────────────┐
│ Raw: spans, logs, etc. │                │ billing config, orgs, invoices    │
│         │              │                │              ▲                    │
│         ▼ [MV]         │                │              │                    │
│ usage_hourly           │───daily───────►│ usage_daily_snapshots             │
│ (real-time aggregates) │    rollup      │ (billing source of truth)         │
└────────────────────────┘                └────────────────────────────────────┘
```

- **ClickHouse materialized views**: Pre-aggregate usage hourly as data arrives. This avoids expensive queries over raw data when users check their usage. Minimal write overhead (~5-10%), massive read savings.
- **PostgreSQL daily snapshots**: The billing worker reads from ClickHouse hourly MVs (fast) and writes daily rollups to PostgreSQL. This is the **source of truth for billing** - used for invoice generation, payment processing, and audit trails.
- **Why PostgreSQL for billing**: Transactional integrity for financial data, easy joins with organizations/invoices/payments, immutable audit trail, simpler billing queries without hitting ClickHouse.
- **Retention strategy**: ClickHouse hourly data can be dropped after 7-30 days (configurable). PostgreSQL daily snapshots kept indefinitely for billing history.

**Forecasting approach**: We'll implement two methods:
1. **Linear extrapolation**: Simple daily average projected to month end. Fast, works well for stable usage.
2. **Weighted average**: Recent days weighted higher than older days. Better for growing/shrinking usage patterns.

Users can see both forecasts with confidence intervals. The system defaults to weighted average but shows variance between methods as a quality indicator.

### Potential Issues

**Accuracy at month start**: With only 1-3 days of data, forecasts based purely on current month have high variance. Mitigation: Use previous months' data as a baseline. For example:
- Days 1-7: Weight forecast 70% on previous 3-month average, 30% on current month trend
- Days 8-14: Weight 50/50
- Days 15+: Weight primarily on current month trend
This gives stable early-month forecasts while still adapting to actual usage as data accumulates.

**Bursty workloads**: Some users have predictable weekly spikes (e.g., Monday batch jobs). Linear/weighted averaging doesn't capture this. Future enhancement: Add day-of-week seasonality detection.

**Anomaly handling during incidents**: A major outage could temporarily reduce traffic, then spike when resolved, skewing forecasts. Mitigation:
- Allow users to mark incident windows for exclusion from forecast baseline
- Alert when forecast variance suddenly increases (indicates unstable prediction)
- Provide "exclude last N hours" option for manual adjustment

**Budget enforcement vs. visibility**: This system provides **visibility only**, not hard enforcement. We will:
- Show forecasts and alerts when approaching/exceeding budgets
- **Not** automatically stop data ingestion when budget exceeded
- Rationale: Stopping ingestion during an incident would be catastrophic. Budget enforcement is a billing/commercial concern, not an ingestion concern.
- Future consideration: Optional "hard limit" mode for cost-sensitive customers (with prominent warnings about data loss risk)

**Real-time vs. snapshot**: ClickHouse materialized views provide near-real-time aggregation, but there's inherent delay. We'll document that usage shown may lag by up to 1 hour.

**Multi-currency**: Initial implementation is USD-only. Internationalization deferred to future iteration.

### Affected Components

- New module: `src/billing/` (types, usage queries, forecasting logic, worker)
- New API: `src/api/billing.rs`
- New migration: `migrations/005_usage_billing.sql`
- Modified: `src/main.rs` (spawn billing worker), `src/app_state.rs` (add BillingService)

---

## 2. Notification Settings for Billing Alerts

### What It Is

User-configurable notification preferences for billing-related alerts: budget thresholds, forecast warnings, usage spikes, and limit approaching notifications.

### Why We're Adding This

**User control**: Different users have different tolerance for notifications. Finance teams want budget alerts; engineers might only care about limit warnings.

**Reducing noise**: Without granular controls, users either get too many alerts (and ignore them) or disable notifications entirely (and miss important ones).

**Per-user preferences**: In multi-user organizations, not everyone needs every alert. Billing alerts should go to admins/finance, not individual developers.

### How It Works Within Existing Code

**Leverages existing notification system**: Reiver already has `src/alerts/notifier.rs` with support for Slack, PagerDuty, Teams, Discord, and webhooks. Billing alerts will use the same dispatch mechanism, just with a different payload type.

**Preference storage**: A new `billing_notification_preferences` table stores per-user, per-organization settings. This follows the pattern of other user preferences in the system.

**Alert types**:
| Alert Type | Trigger | Default Threshold |
|------------|---------|-------------------|
| Budget threshold | Current spend exceeds X% of monthly budget | 80% |
| Forecast exceeded | Projected spend will exceed budget | 100% |
| Usage spike | Daily usage is X% higher than 7-day average | 200% |
| Limit approaching | Usage approaching plan limit | 90% |

**Quiet hours**: Users can specify times when non-critical alerts are suppressed (e.g., nights/weekends). Critical alerts (e.g., hard limit reached) always send.

### Potential Issues

**Notification fatigue**: If thresholds are too sensitive, users get too many alerts. Mitigation: Conservative defaults, clear guidance in UI, and "snooze" functionality.

**Organization vs. user scope**: Some alerts (budget exceeded) are organization-wide but should respect individual notification preferences. We'll send to all users who have that alert type enabled, with the org as the subject.

**Channel configuration**: Billing alerts need to know which Slack channel or email to use. We'll reuse the existing `notification_channels` table, with users selecting which channels receive billing alerts.

**Timezone handling**: Quiet hours require timezone awareness. We'll store user timezone preference and convert appropriately.

**Notification rate limiting and deduplication**: Without controls, a threshold crossed repeatedly could flood users with alerts. Mitigation:
- **Cooldown period**: After sending an alert, suppress identical alerts for configurable duration (default: 15 minutes)
- **Aggregation**: If the same threshold is crossed N times during cooldown, send a single summary ("Budget threshold exceeded 47 times in the last hour")
- **Daily digest option**: Users can opt for a daily summary instead of real-time alerts for non-critical thresholds
- **Per-channel limits**: Each notification channel has a max alerts/hour setting (default: 20) to prevent flooding

### Affected Components

- Modified: `src/alerts/notifier.rs` (add billing notification type)
- New API: `src/api/billing_notifications.rs`
- New migration: Extends `migrations/005_usage_billing.sql`
- Integration: Called from billing worker when thresholds exceeded

---

## 3. AI-Powered Root Cause Analysis

### What It Is

Enhanced root cause analysis that correlates multiple telemetry signals (logs, spans, metrics, exceptions) to identify probable causes of incidents, optionally enhanced with LLM-generated explanations.

### Why We're Adding This

**Reduce MTTR**: Mean time to resolution is a critical SRE metric. Competitors claim 10x MTTR reduction with AI-powered root cause analysis. This is becoming table stakes.

**Signal correlation is hard**: Users have logs in one place, traces in another, metrics in a third. Manually correlating them during an incident is time-consuming and error-prone.

**Accessibility**: Natural language explanations make root cause analysis accessible to non-expert users (product managers, support teams, executives).

### How It Works Within Existing Code

**Builds on existing foundation**: `src/root_cause.rs` already has `fetch_root_cause_suggestions` that queries log templates. We'll enhance this rather than replace it.

**Multi-signal correlation**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Incident Time Window                          │
├──────────────────┬──────────────────┬──────────────────┬────────┤
│   Log Patterns   │  Span Anomalies  │ Metric Deviations│Exceptions│
│  (existing)      │  (new)           │  (new)           │ (new)    │
└────────┬─────────┴────────┬─────────┴────────┬─────────┴────┬───┘
         │                  │                  │              │
         └──────────────────┴──────────────────┴──────────────┘
                                    │
                            ┌───────▼───────┐
                            │  Correlator   │
                            │  & Ranker     │
                            └───────┬───────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
            ┌───────▼───────┐              ┌───────▼───────┐
            │ Programmatic  │              │  LLM-Enhanced │
            │   Summary     │              │  Explanation  │
            │ (always)      │              │  (optional)   │
            └───────────────┘              └───────────────┘
```

**Analysis approach**:
1. **Baseline comparison**: Compare incident window to a baseline (previous 24 hours) to identify what's different
2. **Span anomalies**: Find operations where latency during incident significantly exceeds normal p99
3. **Metric deviations**: Find metrics that deviated more than 2 standard deviations from baseline
4. **Exception correlation**: Link exceptions that occurred during the incident to affected services
5. **Timeline construction**: Order events chronologically to show incident progression

**Confidence scoring**: Each finding contributes to overall confidence. More corroborating signals = higher confidence. We'll show confidence as a percentage with explanation of contributing factors.

**LLM integration**: Optional feature that sends a structured summary to an LLM and asks for a natural language explanation. The LLM sees aggregated data, not raw logs/traces (for privacy and cost reasons).

**LLM provider strategy** (multi-provider with user choice):

| Provider | Quality | Cost (input/output per 1M tokens) | Best For |
|----------|---------|-----------------------------------|----------|
| OpenAI GPT-4o-mini | Very good | ~$0.15 / $0.60 | Default for cloud, cost-effective |
| Anthropic Claude 3.5 Sonnet | Excellent reasoning | ~$3.00 / $15.00 | Complex technical analysis |
| Anthropic Claude 3 Haiku | Good | ~$0.25 / $1.25 | Fast, budget-friendly |
| Ollama (Llama 3.1 70B) | Good | Free (GPU cost only) | Self-hosted, data never leaves infra |
| AWS Bedrock | Varies | AWS pricing | Enterprise AWS environments |

*Note: Pricing as of January 2026. Verify current rates before implementation.*

**Configuration approach**:
- `AI_PROVIDER`: "openai" | "anthropic" | "ollama" | "bedrock" | "none"
- `AI_API_KEY`: User-provided API key
- `AI_MODEL`: Optional model override

**Defaults by deployment**:
- **Reiver Cloud**: GPT-4o-mini (we pay, built into subscription)
- **Self-hosted Pro**: User provides their own API key
- **Self-hosted Enterprise**: Ollama support for fully local inference (no data leaves infrastructure)

**Why multi-provider**: No vendor lock-in, customers control data residency, self-hosted users don't incur our LLM costs, easy to add new providers as the market evolves.

### Potential Issues

**False positives**: Correlation doesn't imply causation. A deployment that happened before an incident might be coincidental. Mitigation: Show confidence levels, never claim certainty, provide evidence for users to evaluate.

**Baseline selection**: What's "normal"? For new services, there's no baseline. Mitigation: Require minimum data (e.g., 24 hours) before enabling anomaly detection; fall back to static thresholds.

**LLM costs**: Each AI explanation costs money. Mitigation: Make it optional, cache results, rate limit requests.

**LLM accuracy**: LLMs can hallucinate or provide incorrect analysis. Mitigation: Frame as "suggestion" not "diagnosis", always show underlying data, include disclaimer.

**Query performance**: Joining across logs, spans, and metrics for large time windows could be slow. Mitigation: Limit time window to 4 hours max, use sampling for large datasets.

### Affected Components

- Modified: `src/root_cause.rs` (major enhancement)
- New: `src/root_cause/analyzer.rs`, `src/root_cause/correlator.rs`
- New API endpoint: `POST /api/projects/{id}/root-cause/analyze`
- Optional dependency: OpenAI API for LLM explanations (configured via environment variable)

---

## 4. LLM/AI Agent Observability

### What It Is

Extended observability capabilities specifically designed for AI/LLM applications, including multi-agent workflows, tool calling, RAG pipeline monitoring, and prompt versioning.

### Why We're Adding This

**Market timing**: 98% of organizations expect to use GenAI in observability by 2026. 2025-2026 is being called "The Year of AI Agents." This is the fastest-growing segment of observability.

**Existing foundation**: Reiver already has `src/llm/` module with cost calculation, session tracking, and GenAI semantic convention support. We're extending, not building from scratch.

**Differentiation**: Datadog and New Relic are bolting on AI observability. We can build it natively and better.

### How It Works Within Existing Code

**Extends GenAI semantic conventions**: OpenTelemetry has emerging conventions for AI/LLM. We'll add support for agent-specific attributes:

| Attribute Category | Purpose |
|-------------------|---------|
| Agent identification | Track which agent executed, its version and type |
| Handoffs | Trace when one agent delegates to another |
| Tool calls | Monitor external tool invocations (APIs, databases, functions) |
| RAG context | Track retrieval quality, latency, and relevance scores |
| Guardrails | Monitor when safety guardrails trigger |
| Prompt versioning | Track which prompt template/version was used |

**Data model**:

```
Trace
└── Agent Span (orchestrator)
    ├── LLM Request (routing decision)
    ├── Agent Span (worker 1) ─── Handoff
    │   ├── Tool Call (API)
    │   └── LLM Request (with RAG context)
    └── Agent Span (worker 2) ─── Handoff
        ├── Tool Call (database)
        └── LLM Request (final response)
```

**Storage**:
- Agent spans go to ClickHouse (high volume, time-series queries)
- Tool calls stored separately for per-tool analytics
- RAG queries tracked for retrieval quality analysis
- Prompt versions stored in PostgreSQL (low volume, needs versioning)

**Visualization**: New "Agent Flow" view shows multi-agent traces as a graph, with nodes (agents) and edges (handoffs), annotated with costs and latency.

### Potential Issues

**Semantic convention stability**: OpenTelemetry GenAI conventions are still evolving. We may need to update attribute names as standards mature. Mitigation: Abstract attribute names behind constants, document our supported version.

**Instrumentation burden**: Users need to instrument their agent code. Mitigation: Provide SDK helpers, auto-instrumentation for popular frameworks (LangChain, CrewAI, AutoGen).

**Data volume**: AI agents can generate many spans per user interaction. Mitigation: Sampling support, aggregation at ingestion time.

**Tool output size**: Tool call outputs (e.g., database query results) can be large. Mitigation: Truncate/summarize large outputs, configurable capture limits.

**Privacy concerns**: Prompts and responses may contain PII. Mitigation: Respect existing content capture settings, add agent-specific content filtering options.

### Affected Components

- Extended: `src/llm/types.rs` (add agent attributes)
- Extended: `src/llm/processor.rs` (extract agent data from spans)
- New: `src/api/agents.rs` (agent-specific API endpoints)
- New ClickHouse tables: `agent_spans`, `tool_calls`, `rag_queries`
- New PostgreSQL table: `prompt_versions`
- New migration: `migrations/006_agent_observability.sql`

---

## 5. Datadog Migration Tools

### What It Is

Tools to help users migrate from Datadog to Reiver: dashboard import, alert rule import, and compatibility layer for Datadog agent format.

### Why We're Adding This

**Reduce switching friction**: The biggest barrier to switching observability platforms is the sunk cost of existing dashboards and alerts. If we can import them, the barrier drops significantly.

**Capture market discontent**: Datadog pricing complaints are widespread. Users actively searching for alternatives need a migration path.

**Competitive positioning**: "Migrate from Datadog in 30 minutes" is a powerful marketing message.

### How It Works Within Existing Code

**Dashboard conversion**:

```
Datadog Dashboard JSON          Reiver Dashboard
┌────────────────────┐         ┌────────────────────┐
│ title              │ ──────► │ name               │
│ widgets[]          │         │ widgets[]          │
│   - definition     │ ──────► │   - widget_type    │
│   - layout         │ ──────► │   - position/size  │
│ template_variables │         │ (future)           │
└────────────────────┘         └────────────────────┘
```

**Widget type mapping**:
| Datadog Widget | Reiver Equivalent | Notes |
|----------------|-------------------|-------|
| timeseries | timeseries | Direct mapping |
| query_value | metric | Single value display |
| toplist | table | Ranked list |
| heatmap | heatmap | Direct mapping |
| note | text | Markdown content |
| group | tab | Container widget |

**Query translation**: Datadog's query language differs from Reiver's. Examples:
- Datadog: `avg:system.cpu.user{host:web-*} by {host}`
- Reiver: `avg(system.cpu.user) where host like 'web-%' group by host`

We'll parse the Datadog query, extract components (aggregation, metric, filters, grouping), and generate Reiver-equivalent queries. For complex queries that can't be translated, we'll preserve the original as a comment and flag for manual review.

**Query feature compatibility matrix**:

| Datadog Feature | Support Level | Notes |
|-----------------|---------------|-------|
| Basic aggregations (avg, sum, min, max, count) | Full | Direct translation |
| Tag filtering (`{tag:value}`) | Full | Maps to WHERE clause |
| Grouping (`by {tag}`) | Full | Maps to GROUP BY |
| Arithmetic between metrics | Partial | Simple operations supported |
| `rollup()` function | Partial | Maps to time-bucket aggregation |
| `as_count()`, `as_rate()` | Partial | Best-effort conversion |
| `fill()` function | None | Not supported, flagged for review |
| Forecast/anomaly functions | None | Datadog-specific, not translated |
| Composite conditions (a && b) | Partial | Simple AND/OR supported |
| Nested functions | Limited | Single-level nesting only |

Unsupported features will be preserved as comments in the imported dashboard for manual review.

**Agent compatibility**: Accept metrics in Datadog agent format via `/api/v1/series` endpoint. This allows users to run Reiver alongside Datadog during migration, or use existing Datadog agents/integrations temporarily.

### Potential Issues

**Query translation completeness**: Datadog's query language is complex with functions, formulas, and conditional logic. We can't translate 100%. Mitigation: Focus on common patterns (covers 80% of dashboards), flag untranslatable queries for manual review, preserve original for reference.

**Semantic differences**: Some Datadog concepts don't have direct Reiver equivalents (e.g., template variables, custom time zones per widget). Mitigation: Document limitations, skip unsupported features with warnings.

**Dashboard API access**: Getting dashboard JSON from Datadog requires their API. We can't pull directly; users must export. Mitigation: Provide clear instructions, accept file upload, support API token input for direct fetch (with user consent).

**Maintenance burden**: Datadog changes their format occasionally. Mitigation: Version the importer, focus on stable/common patterns, treat edge cases as best-effort.

### Affected Components

- New module: `src/migration/datadog/` (dashboard converter, alert converter, query parser)
- New API: `src/api/migration.rs`
- New API: `src/api/datadog_compat.rs` (agent compatibility endpoint)
- Modified: `src/api.rs` (mount new routes)

---

## 6. Continuous Profiling with Trace Correlation

### What It Is

Complete the OpenTelemetry profiling implementation and add advanced features: profile-to-trace linking and deployment/version comparison.

### Why We're Adding This

**Complete the picture**: Traces show what happened, profiles show why it was slow. Together they answer "this endpoint is slow because function X is consuming 40% of CPU."

**Existing foundation**: `PROFILING_IMPLEMENTATION_PLAN.md` documents that opentelemetry-proto 0.31.0 includes profiling types, and `src/api/profiles.rs` has basic endpoints. We need to finish and enhance.

**Deployment awareness**: Comparing profiles between versions answers "did the new release make things faster or slower?"

### How It Works Within Existing Code

**Profile-to-trace correlation**:
1. Profiling data includes optional `trace_id` and `span_id` attributes
2. When viewing a slow span, UI can query for associated profiles
3. When viewing a profile, UI can link to the originating trace

**Profile comparison**:

```
Version A (baseline)              Version B (comparison)
┌─────────────────────┐          ┌─────────────────────┐
│ main()        100%  │          │ main()        100%  │
│ ├─ handler()   60%  │          │ ├─ handler()   45%  │ ◄─ 15% improvement
│ │  ├─ db()     40%  │          │ │  ├─ db()     25%  │ ◄─ 15% improvement
│ │  └─ cache()  20%  │          │ │  └─ cache()  20%  │    (no change)
│ └─ auth()      40%  │          │ └─ auth()      55%  │ ◄─ 15% regression
└─────────────────────┘          └─────────────────────┘
```

**Comparison modes**:
1. **Version-to-version**: Compare profiles tagged with `service.version=1.0` vs `service.version=1.1`
2. **Time-period**: Compare profiles from last hour vs previous day (for investigating incidents)

**Output**: Diff report showing:
- Overall CPU/memory change percentage
- Functions with significant regressions (sorted by impact)
- Functions with significant improvements
- New functions (didn't exist in baseline)
- Removed functions (existed in baseline, not in comparison)

### Potential Issues

**Profile data volume**: Continuous profiling generates significant data. Mitigation: Sampling (1 in 100 requests), short retention (7 days default), aggregation for older data.

**Flame graph rendering**: Large profiles can have thousands of functions. Client-side rendering may be slow. Mitigation: Server-side aggregation, lazy loading, focus on top N functions.

**Version tagging**: Comparison requires services to include version in resource attributes. Many don't. Mitigation: Document requirement, provide SDK helpers, fall back to time-based comparison.

**Statistical significance**: Comparing two profiles doesn't account for variance. A 5% difference might be noise. Mitigation: Require minimum sample count, show confidence indicators, aggregate multiple profiles.

### Affected Components

- Complete: `src/api/profiles.rs` (add comparison endpoint)
- Modified: `src/api/otlp.rs` (ensure profiling ingestion works end-to-end)
- New: Profile comparison logic (aggregate, diff, report)
- Modified: Frontend to show comparison UI (out of scope for this doc)

---

## 7. UX Simplicity

### What It Is

Reduce time-to-value with auto-detection, intelligent defaults, and guided onboarding. Goal: useful dashboard in 5 minutes from first data.

### Why We're Adding This

**Competitive weakness of incumbents**: Datadog requires "weeks of training" per user reviews. Complex tagging systems overwhelm new users.

**Onboarding is critical**: Users who don't see value in the first session often don't return. First impressions matter.

**Reduce support burden**: Self-service onboarding means fewer "how do I..." support tickets.

### How It Works Within Existing Code

**Auto-detection pipeline**:

```
First spans arrive
       │
       ▼
┌──────────────────┐
│ Detect Services  │  Query distinct service_name from spans
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Infer Types      │  web, api, worker, database, cache, queue
└────────┬─────────┘  Based on name patterns and operation types
         │
         ▼
┌──────────────────┐
│ Generate Alerts  │  Service-specific defaults
└────────┬─────────┘  (error rate, latency, type-specific)
         │
         ▼
┌──────────────────┐
│ Build Dashboard  │  Overview + per-service widgets
└──────────────────┘
```

**Service type inference**:
| Pattern | Inferred Type |
|---------|---------------|
| Name contains "web", "frontend" | Web |
| Name contains "api", "gateway" | API |
| Name contains "worker", "consumer", "job" | Worker |
| Name contains "postgres", "mysql", "mongo" | Database |
| Name contains "redis", "memcache" | Cache |
| Operations like "GET /", "POST /" | API |

**Intelligent defaults**:
- Error rate alert at 5% (not 1% which would be noisy)
- Latency alert at 2x current p99 (adapts to service baseline)
- 1-hour default time range (shows recent without overwhelming)
- Auto-refresh at 60 seconds

**Onboarding flow**:
1. **Send data** - Detect when first spans arrive, show success
2. **Review services** - Show detected services, let user confirm/edit types
3. **Configure alerts** - Present suggested alerts, one-click enable
4. **View dashboard** - Auto-generated overview, can customize later

**Progress tracking**: Store onboarding state in project settings. Show progress indicator until complete. Don't nag completed users.

### Potential Issues

**Over-simplification**: Power users may find defaults limiting. Mitigation: Make defaults a starting point, not a constraint. Easy path to customization.

**Incorrect inference**: Service type detection can be wrong (e.g., "redis-proxy" is API, not cache). Mitigation: Let users edit, learn from corrections.

**Sparse data**: Auto-detection needs enough data to work. With only 10 spans, we can't reliably detect patterns. Mitigation: Require minimum data (e.g., 100 spans) before suggesting, show "waiting for more data" state.

**Alert noise**: Auto-generated alerts might not match user needs. Mitigation: Start disabled or in "preview" mode, let users enable after review.

### Affected Components

- New: `src/onboarding/auto_detect.rs`
- New API: `src/api/onboarding.rs`
- Modified: Project creation flow to trigger detection
- Modified: Dashboard creation to support auto-generation
- New: Onboarding state storage (in `projects.settings` JSON)

---

## 8. Self-Hosted Licensing

### What It Is

A licensing system for self-hosted Reiver deployments that validates license keys, gates features by tier, and optionally reports usage for compliance.

### Why We're Adding This

**Business model**: Reiver is not open source. Self-hosted deployments need a mechanism to enforce licensing and enable paid features.

**Feature differentiation**: Different tiers get different features. Enterprise gets SSO/SCIM, Pro gets AI features, Community gets basics.

**Compliance**: For enterprise customers, we need to track that deployments are within license terms (user count, data volume).

### How It Works Within Existing Code

**License key structure**: License keys are signed JWTs containing:
- Organization identifier
- Tier (Community, Pro, Enterprise)
- Enabled features list
- Limits (max users, max projects, max spans/day)
- Expiration date

**Cryptographic verification**:
- **Signing algorithm**: Ed25519 (fast, secure, small keys)
- **Key distribution**: Reiver's public verification key is embedded in the binary at compile time
- **Offline validation**: No network call required - signature verified locally using embedded public key
- **Key rotation**: 
  - Public key embedded in binary means rotation requires binary update
  - We'll maintain backward compatibility by supporting multiple public keys during transition periods
  - Major version releases may deprecate old keys with advance notice
- **Signing key security**: Private signing key stored in HSM, accessible only to license generation service

**Validation flow**:

```
Startup                              Runtime
   │                                    │
   ▼                                    ▼
┌──────────────┐                 ┌──────────────────┐
│ Load license │                 │ API request      │
│ from config  │                 │ to gated feature │
└──────┬───────┘                 └────────┬─────────┘
       │                                  │
       ▼                                  ▼
┌──────────────┐                 ┌──────────────────┐
│ Verify       │                 │ Check feature    │
│ signature    │                 │ in license       │
└──────┬───────┘                 └────────┬─────────┘
       │                                  │
       ▼                                  ▼
┌──────────────┐                 ┌──────────────────┐
│ Check expiry │                 │ Allow or reject  │
└──────┬───────┘                 │ with 403         │
       │                         └──────────────────┘
       ▼
┌──────────────┐
│ Store in     │
│ AppState     │
└──────────────┘
```

**Feature tiers**:

| Feature | Community | Pro | Enterprise |
|---------|:---------:|:---:|:----------:|
| Traces, logs, metrics | ✓ | ✓ | ✓ |
| Basic dashboards | ✓ | ✓ | ✓ |
| LLM observability | - | ✓ | ✓ |
| Continuous profiling | - | ✓ | ✓ |
| AI root cause analysis | - | ✓ | ✓ |
| SSO (SAML/OIDC) | - | - | ✓ |
| SCIM provisioning | - | - | ✓ |
| Audit logs | - | - | ✓ |
| Custom retention | - | - | ✓ |
| Priority support | - | - | ✓ |

**Feature gating implementation**: Middleware checks license before allowing access to gated routes. Returns 403 with upgrade message if feature not included.

**Telemetry** (opt-in): Anonymous usage reporting helps us understand deployment patterns and verify compliance. Sends only: license ID, version, user/project counts, aggregate data volumes. No PII, no telemetry data content. Disabled by default; enterprise customers can enable for better support.

### Potential Issues

**Offline deployments**: Some enterprises have air-gapped environments. License validation must work offline. Mitigation: Signed JWTs validate locally without network call. Expiration is the only time-based check.

**Clock manipulation**: Users could set system clock back to extend expired licenses. Mitigation: For high-value enterprise licenses, optional phone-home check (can be disabled).

**Key sharing**: Nothing technically prevents copying license keys between deployments. Mitigation: License to specific organization, include deployment limits, monitor for anomalies in telemetry.

**Graceful degradation**: If license expires, what happens? Mitigation: Warn starting 30 days before expiry. After expiry, gated features return 403 but basic functionality continues. No data loss.

**License key distribution**: How do customers get keys? Mitigation: Customer portal (future), manual issuance initially.

### Affected Components

- New module: `src/licensing/` (validation, feature gating, telemetry)
- Modified: `src/app_state.rs` (hold license state)
- Modified: `src/main.rs` (load and validate license on startup)
- New middleware: License feature check
- Modified: Gated routes (SSO, SCIM, etc.) to check license
- New API: `src/api/license.rs` (status, activation)
- New config: `REIVER_LICENSE_KEY` environment variable

---

## Implementation Priorities

Based on competitive differentiation value and existing foundation:

### High Priority (Immediate)
1. **Cost Forecasting** - Addresses #1 competitor complaint
2. **Notification Settings** - Required for cost forecasting to be useful

### Medium Priority (Next Quarter)
3. **LLM/AI Agent Observability** - Major market trend, we have foundation
4. **Root Cause Analysis Enhancement** - Builds on existing code
5. **Self-Hosted Licensing** - Required for commercial self-hosted

### Lower Priority (Future)
6. **Datadog Migration** - Important but not blocking
7. **Continuous Profiling** - Enhancement to existing
8. **UX Simplicity** - Ongoing improvements

---

## Dependencies and Risks

### External Dependencies
- OpenAI/Anthropic API for AI features (optional, user-provided key)
- No new Rust crates required for core features

### Technical Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| ClickHouse query performance at scale | Medium | High | Test with production-like data volumes early |
| LLM API reliability | Medium | Low | Feature is optional, graceful fallback |
| License key cryptography bugs | Low | High | Use well-tested libraries (ring), security review |
| Dashboard import edge cases | High | Low | Best-effort import, preserve original |

### Resource Requirements

**Backend estimates by feature** (focused development, single engineer):

| Feature | MVP Estimate | Full Implementation | Notes |
|---------|--------------|---------------------|-------|
| 1. Cost Forecasting | 1 week | 2 weeks | MVs exist pattern in codebase |
| 2. Notification Settings | 0.5 weeks | 1 week | Extends existing notifier |
| 3. AI Root Cause Analysis | 1.5 weeks | 3 weeks | LLM integration adds complexity |
| 4. LLM/AI Agent Observability | 1 week | 2 weeks | Extends existing LLM module |
| 5. Datadog Migration | 2 weeks | 4 weeks | Query translation is the long pole |
| 6. Continuous Profiling | 1 week | 2 weeks | Foundation exists per PROFILING_IMPLEMENTATION_PLAN.md |
| 7. UX Simplicity | 1 week | 2 weeks | Backend portion only |
| 8. Self-Hosted Licensing | 1 week | 2 weeks | Crypto and gating middleware |
| **Total** | **9 weeks** | **18 weeks** | |

**Notes on estimates**:
- MVP = core functionality working end-to-end, happy path only
- Full = edge cases handled, error handling robust, production-ready
- Estimates assume no major architectural surprises
- Testing time included in estimates above
- Parallelization possible for independent features (1-2, 3-4, 5-6)

**Other resources**:
- Frontend: Parallel work needed for UI components (not scoped here)
- DevOps: New ClickHouse tables, migration testing (~1 week)

---

## Open Questions (with Tentative Recommendations)

1. **Forecast algorithm**: Should we add ML-based forecasting (e.g., Prophet) or keep it simple?
   - **Recommendation**: Start with weighted average (Section 1). Evaluate ML-based forecasting post-MVP only if forecast variance remains high (>20% error rate). ML adds complexity and dependencies that aren't justified without data showing simple methods are insufficient.

2. **LLM provider**: Support multiple providers (OpenAI, Anthropic, local) or start with one?
   - **Recommendation**: Multi-provider from day one (as described in Section 3). The abstraction cost is low, and customer requirements vary significantly. Start with OpenAI as default, Ollama for self-hosted.

3. **License key rotation**: How do customers rotate keys without downtime?
   - **Recommendation**: Support dual-key validation during transition periods. When a new license is issued, both old and new keys are valid for a 7-day overlap period. The system accepts either key, and logs a warning when the old key is used after the new one is issued.

4. **Dashboard import scope**: Should we also import Grafana dashboards?
   - **Recommendation**: Defer to future iteration. Datadog is the primary migration target based on market positioning. Grafana import can be added if customer demand materializes, but the Grafana-to-Reiver migration story is weaker (Grafana users often self-host and have different concerns).

---

## Testing Strategy

Each feature requires appropriate test coverage before release:

### Unit Tests
- **Billing calculations**: Verify forecast algorithms produce expected results with known inputs
- **Query translation**: Test Datadog-to-Reiver query conversion for each supported pattern
- **License validation**: Test JWT signature verification, expiry handling, feature gating
- **Service type inference**: Test pattern matching for auto-detection

### Integration Tests
- **Billing worker**: Verify ClickHouse MV → PostgreSQL snapshot flow end-to-end
- **Notification dispatch**: Test each channel type (Slack, PagerDuty, etc.) with mock endpoints
- **OTLP profiling ingestion**: Test profile data storage and retrieval
- **Root cause correlation**: Test multi-signal queries return expected results

### Load/Performance Tests
- **Billing queries at scale**: Test forecast queries with production-like data volumes (100M+ spans)
- **Dashboard import**: Test with large Datadog dashboards (50+ widgets)
- **Profile storage**: Verify ClickHouse performance with continuous profiling data volume

### Accuracy Tests (Billing-specific)
- **Forecast accuracy tracking**: Compare predictions to actuals over time
- **Usage counting accuracy**: Verify counts match between ClickHouse and PostgreSQL
- **No data loss**: Verify billing snapshots capture all ingested data

### Security Tests
- **License tampering**: Verify modified JWTs are rejected
- **Feature gating bypass**: Verify gated endpoints return 403 without valid license
- **PII in LLM context**: Verify AI features don't leak sensitive data to external APIs

---

## Appendix: Related Documents

- [PROFILING_IMPLEMENTATION_PLAN.md](../PROFILING_IMPLEMENTATION_PLAN.md) - Detailed profiling implementation
- [README.md](../README.md) - Current Reiver architecture overview
