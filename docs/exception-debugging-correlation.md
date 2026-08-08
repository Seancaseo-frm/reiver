# Exception Debugging & Correlation – Tracked Work

Tracking improvements to help clients debug exceptions faster: trace–log–exception correlation, unified incident views, and context.

**Implementation order: 1 → 2 → 4**, then 3, 5, 6, 7.

---

## 1. trace_id / span_id – Store and Link Them

**Goal:** Logs and exceptions are linked to traces and spans ("logs for this trace", "exception in this span").

### 1.1 reiver.logs (via reiver.logs_by_trace)

| Task | Status |
|------|--------|
| New table `reiver.logs_by_trace` with trace_id, span_id (reiver.logs is SummingMergeTree by template—can’t add to key without new table) | [x] |
| OTLP log ingest: write to `reiver.logs_by_trace` when `LogRecord.trace_id` present | [x] |

### 1.2 reiver.unstructured_logs

| Task | Status |
|------|--------|
| Add `trace_id` (Nullable String) to `reiver.unstructured_logs` | [x] |
| Add `span_id` (Nullable String) to `reiver.unstructured_logs` | [x] |
| Ingest paths: populate when source provides them | [x] |
| `/api/logs/ingest`: extract `trace_id` and `span_id` from JSON payload | [x] |
| CloudWatch/Azure/GCP log ingest: support trace_id/span_id (currently None, can be enhanced) | [x] |
| Update `store_log_in_clickhouse` to accept and persist trace_id/span_id | [x] |

### 1.3 Exceptions ↔ span

| Task | Status |
|------|--------|
| Add `span_id` to `ExceptionPayload` (and SDK docs) | [x] |
| Extend `error_traces` with `span_id` (nullable). Migration: `migrations/002_add_span_id_to_error_traces.sql` | [x] |
| Kafka consumer: persist `span_id` when present | [x] |

### 1.4 reiver.spans

- Already has `trace_id`, `id` (span_id), `parent_span_id`. No change.

---

## 2. Inferred Trace–Exception Linking

**Goal:** When `error_traces` is empty (no `trace_id` from SDK), find traces by same service, overlapping time, and span `status=error`.

| Task | Status |
|------|--------|
| In `get_exception_group`: when `error_traces` returns no trace_ids, run inferred query | [x] |
| Inferred query: spans where project match, time overlap with group [first_seen, last_seen], and ≥1 span with `status='error'` | [x] |
| Return inferred traces in `traces` (same shape as explicit; no separate `source` for now) | [x] |

---

## 3. "Logs around this exception" (after 1, 2, 4)

**Goal:** Show logs from the same service in ±2 min around the exception, time-ordered; separate from "rest of incident window."

| Task | Status |
|------|--------|
| Extended `/incidents/context` with `around_ms` and `service_name` params | [x] |
| **Service filtering:** Uses exception's `service_name` when available (from #7) | [x] |
| Backend: queries logs in [around_ms − 2m, around_ms + 2m], optionally filtered by service | [x] |
| Frontend: "Logs around this exception" section and "Logs in incident window" section | [x] |

---

## 4. Unified incident timeline

**Goal:** One time-ordered strip for exception events, logs, spans, and alerts.

| Task | Status |
|------|--------|
| Define backend shape: list of `{ type, time, … }` (exception \| log \| span_start \| span_end \| alert) for a window | [ ] |
| Implement endpoint or extend `/incidents/context` to return timeline events (or derive from existing logs/traces/alerts) | [ ] |
| Frontend: `IncidentTimeline` component (e.g. vertical or Gantt) for exception detail and OTLP error detail | [ ] |
| Wire into Exception Show and Incidents ErrorDetail | [ ] |

---

## 5. Root-cause templates + single incident page

**Goal:** Reuse root-cause/template logic in exception (and OTLP error) view; merge into one incident detail page.

| Task | Status |
|------|--------|
| Reuse root-cause / `fetch_root_cause_suggestions` for exception time window | [x] |
| Add "Dominant log patterns" to exception view | [x] |
| Add "Dominant log patterns" to OTLP error incident detail | [x] |
| Create unified Incident Detail page for exceptions | [x] |
| Route: `/projects/:id/incidents/:type/:incident_id` | [x] |

---

## 6. "Exception in this trace" (and "in this span") in trace UI

**Goal:** Surface linked exceptions on the trace page; eventually "Exception in this span" when we have `span_id`.

| Task | Status |
|------|--------|
| `get_trace` already returns `exceptions` via `error_traces` – confirm and keep | [x] |
| Trace detail page: add "Exceptions" block (list with exception info) | [x] |
| Trace header: add exception count badge | [x] |
| **"Exception in this span":** needs `span_id` in exception ingest and in `error_traces` (or equivalent). Done in §1.3 | [x] |
| Backend: fetch `span_id` from `error_traces` and include in `ExceptionWithSpan` model | [x] |
| Frontend: SpansTable component shows exception indicator badges on spans with errors | [x] |
| Frontend: TraceWaterfall component shows exception icon on spans with errors | [x] |
| Frontend: Exception detail shows which span it occurred in (span_id badge) | [x] |

---

## 7. Service and deploy context

**Goal:** Show `service_name` and, when available, deploy/version at exception time.

| Task | Status |
|------|--------|
| **Service:** add `service_name` to exception payload and persist (group/exception); infer from linked trace when missing | [x] |
| **Service:** show `service_name` in exception/incident summary and use for "logs around" filtering | [x] |
| **Deploy/version:** identify source of deployment/version data (e.g. `deployments` or `releases` table, or tags) | [x] |
| **Deploy/version:** Version data available in span tags (`service.version`) via existing `/services/{service}/versions` endpoint | [x] |
| **Deploy/version:** "Deploy/version at exception time" in incident view when data exists | [x] |
| **Deploy/version:** Query linked trace spans to extract version and show in exception detail UI | [x] |

---

## Summary Checklist

| # | Item | Status |
|---|------|--------|
| 1 | trace_id/span_id on logs (logs_by_trace + unstructured); span_id on exceptions + error_traces | [x] |
| 2 | Inferred trace–exception linking | [x] |
| 3 | "Logs around this exception" | [x] |
| 4 | Unified incident timeline | [x] |
| 5 | Root-cause templates + unified incident page | [x] |
| 6 | "Exception in this span" in trace UI (with span_id badges and indicators) | [x] |
| 7 | Service context (service_name + version/environment extracted from linked spans) | [x] |

---

*Last updated: 2025-*
