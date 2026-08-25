# Flow — Session and Identity Contract

Decide what a session and user mean before adding correlation fields. A Reiver session is the smallest meaningful business episode the owner wants to evaluate, label, budget, replay, and improve.

## Required decision

Infer the contract from `gateway_settings.agent_soul` and the application, then present it in **My understanding** and ask only about material gaps or conflicts:

```text
Session and Identity Contract
- Session unit:
- Starts when:
- Ends successfully when:
- Also ends when:
- Idle fallback:
- Stable user ID source:
- Anonymous-user policy:
- Tenant scoping:
```

When authorised, merge the confirmed block into the existing Agent Soul through `gateway update_settings`, normally in `custom_instructions`; preserve useful existing context. Later coding-agent sessions and MCP tokens should read and reuse `gateway_settings.agent_soul` rather than asking the owner again.

Useful defaults:

- maths tutor: one problem, exercise, or coherent learning episode;
- sales: one lead conversation;
- support: one issue or ticket;
- copilot: one user task;
- processor: one job;
- multi-agent system: one business case or workflow.

Do not use a long-lived user/account ID as the session ID. Do not create a new session per LLM turn unless one turn is genuinely the outcome being evaluated.

## Lifecycle behaviour

There is no separate Reiver session-start call. The first accepted Flow request with a new `x-reiver-session-id` becomes the session's first recorded activity. Create or select the ID at the application's real start event, normally by reusing a conversation/task/job ID or creating an opaque backend ID.

Call `POST /api/gateway/v1/sessions/{session_id}/end` at a real terminal event. The call is idempotent, returns `202`, and schedules evaluation after an approximately 30-second ingestion buffer; `202` confirms scheduling, not completed evaluation. A `503 service_unavailable` response means scheduling was not confirmed and must be retried. Reiver's 30-minute idle evaluator remains a crash/abandonment fallback; it is not the primary business definition of completion.

Restart recovery: explicit end holds a short per-session reservation before the delay. If Flow restarts in that window, the immediate enqueue is lost, but the reservation expires well before the 30-minute idle threshold so idle discovery can enqueue the session normally. Verify that the ended session becomes queryable before accepting the integration.

## Identity rules

Use one stable pseudonymous application ID across a user's sessions. Never send email, name, phone number, or another raw personal identifier merely for correlation.

- tenant-scope or namespace IDs when local identifiers could collide;
- define how anonymous users receive a stable pseudonymous ID;
- do not collapse every anonymous user to the same `anonymous` value;
- do not invent anonymous-to-signed-in identity merging during instrumentation.

Agent Soul describes the application. Session labels describe what happened in one session. The user ID identifies who participated across sessions. Keep these concepts separate.

## Required mapping

- Flow conversation: `x-reiver-session-id`.
- Current OTel conversation attribute: `gen_ai.conversation.id`.
- Reiver compatibility attribute: `gen_ai.session.id`.
- Flow user-sticky routing: `x-reiver-user-id`.
- Current per-user gateway analytics: OpenAI-compatible `user` body field.
- Reiver user-correlation attribute: `gen_ai.user.id` (not a current OTel semantic-convention field).

For new code, emit both conversation attributes with the same value until Reiver's compatibility period ends. Do not introduce the older underscore spelling.

## Agent workflow

1. Confirm the Session and Identity Contract before editing code.
2. Add Flow headers and the `user` body field at the shared LLM client boundary.
3. Add the same attributes to relevant spans and structured logs.
4. Ensure logs are exported through an OTel logging bridge and emitted inside active spans.
5. End the session at the confirmed business terminal events.
6. Test one successful episode and, when practical, one abandonment/failure episode.
7. Verify Flow request, Watch trace, and Watch log independently; compare exact values.
8. Prove that a second session for the same test user receives a new session ID while retaining the user ID.

## MCP verification

- `list` with `resource: "sessions"`.
- `get` with `resource: "session"` or `"session_requests"`.
- `list` with `resource: "traces"` and the expected service/time window.
- `search` with `source: "logs"` and an attributes map containing the conversation key/value.
- `execute` with `resource: "session", action: "end"` only when the owner authorised the action and the token has write access.

If a read scope is missing, report the affected evidence as unverified. A Flow session and Watch telemetry are separate stored signals; matching identifiers are the correlation contract.
