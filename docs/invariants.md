# System Invariants

Assertions about the system's behavior that must hold true.
When in doubt, verify these against the code. If code and invariant conflict,
discuss which one needs to change before proceeding.

---

## Pond — NL Query Pipeline

### Security

- Every LLM-generated SQL query is parsed through `sqlparser` with `ClickHouseDialect`
  before execution. No raw LLM output ever reaches ClickHouse without parsing.
- Only `SELECT` statements are allowed. `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`,
  `ALTER`, `EXPLAIN`, and all other statement types are rejected by the validator.
- Multiple statements (semicolon injection) are rejected — exactly one statement is allowed.
- All table names referenced in a query are checked against the project's catalog entries.
  Tables not in the catalog are rejected as unknown.
- CTE names are tracked separately and not flagged as unknown tables.
- After validation, the SQL is additionally rewritten by `validate_and_rewrite_nl_query`
  which enforces project-level data isolation — an NL query can never read another
  project's data or system tables.
- User-facing error messages from the NL pipeline never contain the generated SQL or
  internal schema details. Full SQL and errors are logged at `warn` level for debugging
  but the user sees only generic "failed to generate a valid query" messages.
- The LLM's self-correction context (sent back to the LLM on retry) DOES include the
  generated SQL and error, but this stays within the LLM prompt and is never returned
  to the end user.

### Rate Limiting

- NL queries are rate-limited at the website proxy level (`website/src/proxy.rs`),
  NOT inside Pond.
- The path `/natural-language` is matched BEFORE the generic `/query` path to ensure
  NL requests get the `NlQuery` rate limit type, not the `Analytics` type.
- `NlQuery` has the most restrictive per-minute limit of all rate limit types
  (default: 10/min, 60/hour).

### Retry Logic

- The retry loop makes at most 3 total LLM calls: 1 initial attempt + `MAX_RETRIES` (2) retries.
- On retry, the LLM receives the previous SQL and the error so it can self-correct.
- The retry counter is 1-indexed (`attempt` starts at 1). The loop exits when
  `attempt > MAX_RETRIES` (i.e., after attempt 3).

### LLM Client

- Temperature is set to 0.0 for deterministic SQL generation.
- Max tokens is 2048.
- HTTP timeout per LLM call is 60 seconds.
- The LLM client calls Flow's `/v1/chat/completions` endpoint — it does NOT call
  OpenAI/Anthropic directly. Flow handles provider routing.
- API keys are per-project, stored encrypted in `project_settings`, and decrypted
  at request time.

### Prompt Building

- When the catalog has ≤50 tables, the full schema context is used (table names,
  column names, types, descriptions, row counts).
- When the catalog has >50 tables, a compact schema context is used (table names
  and descriptions only, no column details).
- Row counts are formatted as human-readable strings: "1.5K", "2.3M", "1.1B".

### Future: Vector DB for Schema Retrieval

- In the future, a vector database may be introduced to improve text-to-SQL accuracy.
  Instead of stuffing the entire schema into the prompt (or falling back to compact
  mode at >50 tables), the user's question would be embedded and used to retrieve
  only the most relevant tables/columns via similarity search. This would replace
  the current `MAX_TABLES_FULL_SCHEMA` threshold approach and enable accurate SQL
  generation even for catalogs with hundreds of tables.

### Query Execution

- NL queries have a safety limit of 1000 rows.
- NL queries have a 30-second timeout.
- NL queries have a 50MB memory budget.
- Caching is disabled for NL queries.
- The default model is `gpt-4o` when the user doesn't specify one.

---

## Flow — Adaptive Latency-Based LLM Routing

### Latency Tracker

- The latency tracker uses `tokio::sync::Mutex` (not `RwLock`) because every
  operation mutates state (eviction happens during reads).
- The rolling window default duration is 5 minutes (300 seconds).
- Eviction uses `checked_duration_since` to guard against clock skew — it will
  not panic if timestamps are inconsistent.
- `get_latencies_batch` acquires the mutex exactly once for all providers in the
  batch, not once per provider.

### Routing Decisions

- Provider ordering for fallback uses P95 latency (not P50 or P99).
- Providers with latency data are sorted lowest-P95-first.
- Providers with no latency data are placed at the end, preserving their original order.
- When no latency tracker is configured, candidates are returned in their original order.

### Streaming TTFB Recording

- For streaming requests, TTFB (time-to-first-byte) is recorded, not total stream duration.
- If tokens were received (`ttft > 0`): TTFB duration is recorded.
- If no tokens and there was an error: the full request duration is recorded
  (to penalize the provider in future routing).
- If no tokens and no error (successful empty response): `Duration::ZERO` is produced
  and nothing is recorded (to avoid misleading latency data).
- This logic is duplicated in both the primary and legacy streaming handlers.

### Degradation Detection

- A provider is considered "degraded" when its P99 latency exceeds the
  `p99_fallback_threshold` (default: 30 seconds).

---

## Pond — Storage Tier Lifecycle

### Tier Transitions

- Multi-step transitions always go through Warm as an intermediate step:
  - Hot → Cold goes Hot → Warm first; the next evaluation cycle handles Warm → Cold.
  - Cold → Hot goes Cold → Warm first; the next evaluation cycle handles Warm → Hot.
- Same-tier "transitions" (Hot→Hot, Warm→Warm, Cold→Cold) produce no job.
- `determine_target_tier` matches rules by `age_days >= after_days`, sorted descending.
  The first matching rule wins.
- A partition age below all `after_days` thresholds produces `None` (no target tier).

### Hot → Warm Downgrade

- The hot-to-warm downgrade is done entirely by ClickHouse via
  `INSERT INTO FUNCTION s3(named_collection, filename, format='Parquet') SELECT * FROM table`.
- Pond never loads table data into its own memory during this process. ClickHouse
  streams directly to R2.
- Pond's role is bookkeeping only: creating partition records, recording row counts
  and byte sizes, committing partitions, updating the tier, and dropping ClickHouse tables.
- The R2 object size is retrieved after export for bookkeeping. If this fails, it is
  logged at `warn` level and defaults to 0.
- The `sync_checkpoint` is NOT modified during downgrade — R2 has the same data
  at the same checkpoint as ClickHouse did. Future syncs continue from that checkpoint.
- ClickHouse tables are only dropped AFTER R2 data is committed
  (the export is transactional: pending → committed → drop).

### SQL Injection Defense

- `r2_key` and `s3_collection_name` are validated before being interpolated into
  the `INSERT INTO FUNCTION s3(...)` SQL string. Single quotes and backslashes
  are rejected.

---

## Website — Proxy and Authentication

### Request Flow

- Every proxied request (Pond, Flow, Watch) goes through authentication first.
- Authentication determines the `user_id`, which is forwarded via `X-User-Id` header
  to downstream services.
- Downstream services trust `X-User-Id` and do not perform their own authentication.
- Project access is verified after authentication but before forwarding.

### Rate Limiting

- Rate limiting is applied per authenticated user, not per IP or per project.
- Rate limits use Redis with atomic Lua scripts (INCR + EXPIRE in one call) to
  prevent the race condition where INCR succeeds but EXPIRE fails.
- Both per-minute and per-hour limits are checked. The stricter limit wins.
